package meb

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"strconv"
	"strings"

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

type PredicateFilter struct {
	Type  PredicateFilterType
	Value interface{}
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
	value    []byte // Triple value bytes (needed for inline object decoding)
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
		pattern, ok := filter.Value.(string)
		if !ok {
			return false
		}
		matched, err := regexp.MatchString(pattern, objValue)
		return err == nil && matched

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
		rangeVals, ok := filter.Value.([2]float64)
		if !ok || len(rangeVals) != 2 {
			return false
		}
		return objNum >= rangeVals[0] && objNum <= rangeVals[1]
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
		sID = keys.PackID(m.topicID, keys.UnpackLocalID(sID))
	}
	if oBound {
		oID = keys.PackID(m.topicID, keys.UnpackLocalID(oID))
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
func decodeInlineID(id uint64) any {
	if id&keys.InlineIsNum != 0 {
		// Number type
		if id&keys.InlineNumF32 != 0 {
			return keys.UnpackInlineFloat32(id)
		}
		return keys.UnpackInlineInt32(id)
	}
	// Bool type
	return keys.UnpackInlineBool(id)
}

func (m *MEBStore) scanImpl(opts *scanOptions, processFn func(*scanResult) (Fact, error)) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		scanCtx := opts.ctx
		if scanCtx == nil {
			scanCtx = context.Background()
		}

		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true // Always fetch values (needed for inline object decoding)
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
			// Capture value bytes for inline object decoding
			_ = item.Value(func(val []byte) error {
				result.value = make([]byte, len(val))
				copy(result.value, val)
				return nil
			})
			fact, err := processFn(&result)
			if err != nil {
				yield(Fact{}, err)
				return
			}

			if !yield(fact, nil) {
				return
			}
		}
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

	return m.scanImpl(opts, func(r *scanResult) (Fact, error) {
		return m.resolveFactStrings(opts, r)
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

	return m.scanImpl(opts, func(r *scanResult) (Fact, error) {
		fact, err := m.resolveFactStrings(opts, r)
		if err != nil {
			return Fact{}, err
		}

		objectStr := fmt.Sprintf("%v", fact.Object)

		for _, filter := range opts.filters {
			if !evaluatePredicateFilter(objectStr, filter) {
				return Fact{}, nil
			}
		}

		return fact, nil
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

	return m.scanImpl(opts, func(r *scanResult) (Fact, error) {
		fact, err := m.resolveFactStrings(opts, r)
		if err != nil {
			return Fact{}, err
		}

		if len(opts.filters) > 0 {
			objectStr := fmt.Sprintf("%v", fact.Object)
			for _, filter := range opts.filters {
				if !evaluatePredicateFilter(objectStr, filter) {
					return Fact{}, nil
				}
			}
		}

		return fact, nil
	})
}

// ScanWithPruning scans facts with semantic hints pruning.
// entityType: only yield triples matching this entity type (0 = no pruning).
// wantPublic: if true, only yield triples with IsPublic flag.
func (m *MEBStore) ScanWithPruning(s, p, o string, entityType uint16, wantPublic bool) iter.Seq2[Fact, error] {
	opts, err := m.prepareScanWithContext(context.Background(), s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}
	opts.pruneEntityType = entityType
	opts.prunePublic = wantPublic

	return m.scanImpl(opts, func(r *scanResult) (Fact, error) {
		return m.resolveFactStrings(opts, r)
	})
}
