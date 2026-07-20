package meb

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"strconv"
	"strings"

	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

type PredicateFilterType string

const (
	PredicateRegex    PredicateFilterType = "regex"
	PredicateRange    PredicateFilterType = "range"
	PredicateGT       PredicateFilterType = "gt"
	PredicateLT       PredicateFilterType = "lt"
	PredicateGTE      PredicateFilterType = "gte"
	PredicateLTE      PredicateFilterType = "lte"
	PredicateContains PredicateFilterType = "contains"
)

// NumericRange represents a closed [Min, Max] numeric range for PredicateRange filters.
type NumericRange struct {
	Min float64
	Max float64
}

type PredicateFilter struct {
	Type      PredicateFilterType
	Value     interface{} // string for regex/contains; float64 for gt/lt/gte/lte; NumericRange for range
	compiled  *regexp.Regexp
}

// NewPredicateFilter creates a PredicateFilter, pre-compiling regex patterns.
func NewPredicateFilter(filterType PredicateFilterType, value interface{}) (*PredicateFilter, error) {
	f := &PredicateFilter{Type: filterType, Value: value}
	if filterType == PredicateRegex {
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("regex filter value must be a string")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
		}
		f.compiled = re
	}
	return f, nil
}

// MustPredicateFilter creates a PredicateFilter, panicking on invalid input.
func MustPredicateFilter(filterType PredicateFilterType, value interface{}) *PredicateFilter {
	f, err := NewPredicateFilter(filterType, value)
	if err != nil {
		panic(err)
	}
	return f
}

type scanStrategy struct {
	prefix []byte
	index  byte
}

func selectScanStrategy(sBound, pBound, oBound bool, sID, pID, oID uint64) *scanStrategy {
	var strategy scanStrategy

	if sBound {
		strategy.prefix = keys.EncodeTripleSPOPrefix(sID, pID, oID)
		strategy.index = keys.TripleSPOPrefix
	} else if oBound {
		if pBound {
			strategy.prefix = keys.EncodeTripleOPSPrefix(oID, pID, 0)
		} else {
			strategy.prefix = keys.EncodeTripleOPSPrefix(oID, 0, 0)
		}
		strategy.index = keys.TripleOPSPrefix
	} else if pBound {
		strategy.prefix = []byte{keys.TripleSPOPrefix}
		strategy.index = keys.TripleSPOPrefix
	} else {
		strategy.prefix = []byte{keys.TripleSPOPrefix}
		strategy.index = keys.TripleSPOPrefix
	}

	return &strategy
}

func (m *MEBStore) resolveScanIDs(s, p, o string) (sID, pID, oID uint64, sBound, pBound, oBound bool, err error) {
	if s != "" {
		sID, err = m.dict.GetID(s)
		if err != nil {
			return 0, 0, 0, false, false, false, dictError("subject", s, err)
		}
		sBound = true
	}

	if p != "" {
		pID, err = m.dict.GetID(p)
		if err != nil {
			return 0, 0, 0, false, false, false, dictError("predicate", p, err)
		}
		pBound = true
	}

	if o != "" {
		oID, err = m.dict.GetID(o)
		if err != nil {
			return 0, 0, 0, false, false, false, dictError("object", o, err)
		}
		oBound = true
	}

	return sID, pID, oID, sBound, pBound, oBound, nil
}

func dictError(field, value string, err error) error {
	return fmt.Errorf("%s %q not found: %w", field, value, err)
}

type scanResult struct {
	foundSID uint64
	foundPID uint64
	foundOID uint64
	key      []byte
}

type scanOptions struct {
	ctx             context.Context
	s               string
	p               string
	o               string
	sID             uint64
	pID             uint64
	oID             uint64
	sBound          bool
	pBound          bool
	oBound          bool
	strategy        *scanStrategy
	filters         []PredicateFilter
	topicID         uint32 // 0 = use current store topic; non-zero = scan specific topic
	pruneEntityType uint16 // 0 = no pruning; non-zero = prune by entity type
	prunePublic     bool   // if true, only yield triples with IsPublic flag
}

func evaluatePredicateFilter(objValue string, filter PredicateFilter) bool {
	switch filter.Type {
	case PredicateRegex:
		if filter.compiled != nil {
			return filter.compiled.MatchString(objValue)
		}
		return false

	case PredicateContains:
		substr, ok := filter.Value.(string)
		if !ok {
			return false
		}
		return len(objValue) > 0 && len(substr) > 0 &&
			strings.Contains(objValue, substr)

	case PredicateGT, PredicateLT, PredicateGTE, PredicateLTE:
		objNum, err1 := strconv.ParseFloat(objValue, 64)
		thresholdNum, ok2 := filter.Value.(float64)

		if err1 != nil || !ok2 {
			return false
		}

		switch filter.Type {
		case PredicateGT:
			return objNum > thresholdNum
		case PredicateLT:
			return objNum < thresholdNum
		case PredicateGTE:
			return objNum >= thresholdNum
		case PredicateLTE:
			return objNum <= thresholdNum
		}

	case PredicateRange:
		objNum, err := strconv.ParseFloat(objValue, 64)
		if err != nil {
			return false
		}
		rangeVals, ok := filter.Value.(NumericRange)
		if !ok {
			return false
		}
		return objNum >= rangeVals.Min && objNum <= rangeVals.Max
	}
	return false
}

func (m *MEBStore) prepareScanWithContext(ctx context.Context, s, p, o string) (*scanOptions, error) {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return nil, err
	}

	// Pack sID and oID with current topicID for symmetric key lookup.
	// Keys in BadgerDB are stored with topic-packed IDs.
	if sBound {
		sID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(sID))
	}
	if oBound {
		oID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(oID))
	}

	strategy := selectScanStrategy(sBound, pBound, oBound, sID, pID, oID)

	return &scanOptions{
		ctx:      ctx,
		s:        s,
		p:        p,
		o:        o,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		strategy: strategy,
	}, nil
}

// resolveFactStrings resolves scan result IDs back to strings using the dictionary.
// For inline IDs (bit 39 set), decodes the primitive value directly from the ID.
func (m *MEBStore) resolveFactStrings(opts *scanOptions, r *scanResult) (Fact, error) {
	var subject, predicate string
	var object any
	var err error

	if opts.sBound {
		subject = opts.s
	} else {
		subject, err = m.dict.GetString(keys.UnpackLocalID(r.foundSID))
		if err != nil {
			return Fact{}, fmt.Errorf("failed to resolve subject ID %d: %w", r.foundSID, err)
		}
	}

	if opts.pBound {
		predicate = opts.p
	} else {
		predicate, err = m.dict.GetString(r.foundPID)
		if err != nil {
			return Fact{}, fmt.Errorf("failed to resolve predicate ID %d: %w", r.foundPID, err)
		}
	}

	if opts.oBound {
		object = opts.o
	} else if keys.IsInline(r.foundOID) {
		// Inline ID: decode primitive value directly from the ID bits
		object = decodeInlineID(r.foundOID)
	} else {
		objectStr, err := m.dict.GetString(keys.UnpackLocalID(r.foundOID))
		if err != nil {
			return Fact{}, fmt.Errorf("failed to resolve object ID %d: %w", r.foundOID, err)
		}
		object = objectStr
	}

	return Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
	}, nil
}

// decodeInlineID converts an inline ID back to its Go primitive value.
// The caller must have already verified IsInline(id) — if called on a
// non-inline ID the result is undefined. If the type bits (bits 38-37)
// are in an unexpected state, bool is returned as a safe default.
func decodeInlineID(id uint64) any {
	if id&keys.InlineIsNum != 0 {
		// Number type
		if id&keys.InlineNumF32 != 0 {
			return keys.UnpackInlineFloat32(id)
		}
		return keys.UnpackInlineInt32(id)
	}
	// Bool type (or unrecognized — fall through to bool as safe default)
	return keys.UnpackInlineBool(id)
}

// scanBatchSize bounds the number of scan results buffered before a batched
// dictionary resolution. 512 results ≈ 12KB of IDs plus resolved strings —
// negligible even on memory-constrained deployments.
const scanBatchSize = 512

// resolveFactsBatch resolves a chunk of scan results using a single batched
// dictionary lookup (GetStrings) instead of a point lookup per fact.
// Inline-encoded object IDs are decoded without dictionary access.
// Error semantics match resolveFactStrings: a missing dictionary entry fails
// the chunk.
func (m *MEBStore) resolveFactsBatch(opts *scanOptions, results []scanResult) ([]Fact, error) {
	facts := make([]Fact, len(results))

	// Collect unbound IDs needing dictionary resolution.
	idSet := make(map[uint64]struct{})
	var ids []uint64
	addID := func(id uint64) {
		if _, ok := idSet[id]; !ok {
			idSet[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	for i := range results {
		r := &results[i]
		if !opts.sBound {
			addID(keys.UnpackLocalID(r.foundSID))
		}
		if !opts.pBound {
			addID(r.foundPID) // predicates are never topic-packed
		}
		if !opts.oBound && !keys.IsInline(r.foundOID) {
			addID(keys.UnpackLocalID(r.foundOID))
		}
	}

	// Single batched dictionary lookup for the whole chunk.
	var resolved map[uint64]string
	if len(ids) > 0 {
		strs, err := m.dict.GetStrings(ids)
		if err != nil {
			return nil, err
		}
		resolved = make(map[uint64]string, len(ids))
		for i, id := range ids {
			resolved[id] = strs[i]
		}
	}

	lookup := func(id uint64, kind string) (string, error) {
		s := resolved[id]
		if s == "" {
			return "", fmt.Errorf("failed to resolve %s ID %d: %w", kind, id, dict.ErrNotFound)
		}
		return s, nil
	}

	for i := range results {
		r := &results[i]
		var subject, predicate string
		var object any

		if opts.sBound {
			subject = opts.s
		} else {
			s, err := lookup(keys.UnpackLocalID(r.foundSID), "subject")
			if err != nil {
				return nil, err
			}
			subject = s
		}

		if opts.pBound {
			predicate = opts.p
		} else {
			p, err := lookup(r.foundPID, "predicate")
			if err != nil {
				return nil, err
			}
			predicate = p
		}

		switch {
		case keys.IsInline(r.foundOID):
			object = decodeInlineID(r.foundOID)
		case opts.oBound:
			object = opts.o
		default:
			objectStr, err := lookup(keys.UnpackLocalID(r.foundOID), "object")
			if err != nil {
				return nil, err
			}
			object = objectStr
		}

		facts[i] = Fact{Subject: subject, Predicate: predicate, Object: object}
	}

	return facts, nil
}

func (m *MEBStore) scanImpl(opts *scanOptions, processFn func(*scanResult) (*Fact, error)) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		scanCtx := opts.ctx
		if scanCtx == nil {
			scanCtx = context.Background()
		}

		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true // Needed for semantic hint pruning (ShouldPruneTriple)
		it := txn.NewIterator(itOpts)
		defer it.Close()

		for it.Seek(opts.strategy.prefix); it.ValidForPrefix(opts.strategy.prefix); it.Next() {
			select {
			case <-scanCtx.Done():
				yield(Fact{}, scanCtx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.TripleKeySize {
				continue
			}

			var result scanResult
			result.foundSID, result.foundPID, result.foundOID = keys.DecodeTripleKey(key)

			if opts.sBound && result.foundSID != opts.sID {
				continue
			}
			if opts.pBound && result.foundPID != opts.pID {
				continue
			}
			if opts.oBound && result.foundOID != opts.oID {
				continue
			}

			// Semantic hints pruning: skip triples that don't match entity type or flags
			if opts.pruneEntityType > 0 || opts.prunePublic {
				var pruned bool
				_ = item.Value(func(val []byte) error {
					pruned = ShouldPruneTriple(val, opts.pruneEntityType, opts.prunePublic)
					return nil
				})
				if pruned {
					continue
				}
			}

			result.key = key
			fact, err := processFn(&result)
			if err != nil {
				yield(Fact{}, err)
				return
			}
			if fact == nil {
				continue // processFn requested skip (e.g. filter rejection)
			}

			if !yield(*fact, nil) {
				return
			}
		}
	}
}

// scanImplBatched mirrors scanImpl but resolves facts in fixed-size chunks,
// converting per-fact dictionary point lookups into one batched resolution per
// chunk via processBatchFn. Streaming semantics are preserved: at most one
// chunk (scanBatchSize results) is buffered at a time.
//
// Note: scanResult.key is not populated in batched mode — the Badger key buffer
// is only valid until the iterator advances, so it must not be retained across
// iterations. No current processFn implementation reads it.
func (m *MEBStore) scanImplBatched(opts *scanOptions, processBatchFn func([]scanResult) ([]Fact, error)) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		scanCtx := opts.ctx
		if scanCtx == nil {
			scanCtx = context.Background()
		}

		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true // Needed for semantic hint pruning (ShouldPruneTriple)
		it := txn.NewIterator(itOpts)
		defer it.Close()

		chunk := make([]scanResult, 0, scanBatchSize)

		flush := func() bool {
			if len(chunk) == 0 {
				return true
			}
			facts, err := processBatchFn(chunk)
			chunk = chunk[:0]
			if err != nil {
				yield(Fact{}, err)
				return false
			}
			for i := range facts {
				if !yield(facts[i], nil) {
					return false
				}
			}
			return true
		}

		for it.Seek(opts.strategy.prefix); it.ValidForPrefix(opts.strategy.prefix); it.Next() {
			select {
			case <-scanCtx.Done():
				yield(Fact{}, scanCtx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.TripleKeySize {
				continue
			}

			var result scanResult
			result.foundSID, result.foundPID, result.foundOID = keys.DecodeTripleKey(key)

			if opts.sBound && result.foundSID != opts.sID {
				continue
			}
			if opts.pBound && result.foundPID != opts.pID {
				continue
			}
			if opts.oBound && result.foundOID != opts.oID {
				continue
			}

			// Semantic hints pruning: skip triples that don't match entity type or flags
			if opts.pruneEntityType > 0 || opts.prunePublic {
				var pruned bool
				_ = item.Value(func(val []byte) error {
					pruned = ShouldPruneTriple(val, opts.pruneEntityType, opts.prunePublic)
					return nil
				})
				if pruned {
					continue
				}
			}

			chunk = append(chunk, result)
			if len(chunk) >= scanBatchSize {
				if !flush() {
					return
				}
			}
		}
		flush()
	}
}

func (m *MEBStore) Scan(s, p, o string) iter.Seq2[Fact, error] {
	return m.ScanContext(context.Background(), s, p, o)
}

func (m *MEBStore) ScanContext(ctx context.Context, s, p, o string) iter.Seq2[Fact, error] {
	opts, err := m.prepareScanWithContext(ctx, s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}

	return m.scanImplBatched(opts, func(results []scanResult) ([]Fact, error) {
		return m.resolveFactsBatch(opts, results)
	})
}

func (m *MEBStore) ScanWithFilters(s, p, o string, filters []PredicateFilter) iter.Seq2[Fact, error] {
	return m.ScanWithFiltersContext(context.Background(), s, p, o, filters)
}

func (m *MEBStore) ScanWithFiltersContext(ctx context.Context, s, p, o string, filters []PredicateFilter) iter.Seq2[Fact, error] {
	opts, err := m.prepareScanWithContext(ctx, s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}
	opts.filters = filters

	return m.scanImplBatched(opts, func(results []scanResult) ([]Fact, error) {
		facts, err := m.resolveFactsBatch(opts, results)
		if err != nil {
			return nil, err
		}

		// Apply predicate filters on resolved facts (same order as before:
		// resolve first, then filter).
		kept := facts[:0]
		for _, fact := range facts {
			objectStr := fmt.Sprintf("%v", fact.Object)

			match := true
			for _, filter := range opts.filters {
				if !evaluatePredicateFilter(objectStr, filter) {
					match = false
					break
				}
			}
			if match {
				kept = append(kept, fact)
			}
		}

		return kept, nil
	})
}

// ScanInTopic scans facts within a specific topic.
// The TopicID is packed into the ID structure for data locality.
// This enables scanning only the prefix range belonging to the requested topic.
func (m *MEBStore) ScanInTopic(topicID uint32, s, p, o string) iter.Seq2[Fact, error] {
	return m.scanInTopicImpl(context.Background(), topicID, s, p, o, nil)
}

// ScanInTopicContext scans facts within a specific topic with context.
func (m *MEBStore) ScanInTopicContext(ctx context.Context, topicID uint32, s, p, o string) iter.Seq2[Fact, error] {
	return m.scanInTopicImpl(ctx, topicID, s, p, o, nil)
}

func (m *MEBStore) scanInTopicImpl(ctx context.Context, topicID uint32, s, p, o string, filters []PredicateFilter) iter.Seq2[Fact, error] {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}

	// Pack IDs with the specified topic for symmetric lookup
	if sBound {
		sID = keys.PackID(topicID, keys.UnpackLocalID(sID))
	}
	if oBound {
		oID = keys.PackID(topicID, keys.UnpackLocalID(oID))
	}

	strategy := selectScanStrategy(sBound, pBound, oBound, sID, pID, oID)

	// If no bound args, use topic-specific prefix scan
	if !sBound && !oBound && !pBound {
		strategy.prefix = keys.EncodeSPOByTopic(topicID)
		strategy.index = keys.TripleSPOPrefix
	}

	opts := &scanOptions{
		ctx:      ctx,
		s:        s,
		p:        p,
		o:        o,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		strategy: strategy,
		filters:  filters,
		topicID:  topicID,
	}

	return m.scanImplBatched(opts, func(results []scanResult) ([]Fact, error) {
		facts, err := m.resolveFactsBatch(opts, results)
		if err != nil {
			return nil, err
		}

		if len(opts.filters) == 0 {
			return facts, nil
		}

		kept := facts[:0]
		for _, fact := range facts {
			objectStr := fmt.Sprintf("%v", fact.Object)
			match := true
			for _, filter := range opts.filters {
				if !evaluatePredicateFilter(objectStr, filter) {
					match = false
					break
				}
			}
			if match {
				kept = append(kept, fact)
			}
		}

		return kept, nil
	})
}

// ScanWithPruning scans facts with semantic hints pruning.
// entityType: only yield triples matching this entity type (0 = no pruning).
// wantPublic: if true, only yield triples with IsPublic flag.
func (m *MEBStore) ScanWithPruning(ctx context.Context, s, p, o string, entityType uint16, wantPublic bool) iter.Seq2[Fact, error] {
	opts, err := m.prepareScanWithContext(ctx, s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}
	opts.pruneEntityType = entityType
	opts.prunePublic = wantPublic

	return m.scanImplBatched(opts, func(results []scanResult) ([]Fact, error) {
		return m.resolveFactsBatch(opts, results)
	})
}
