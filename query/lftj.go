package query

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

// RelationPattern defines a single triple relation to join.
// Prefix: keys.TripleSPOPrefix or keys.TripleOPSPrefix
// BoundPositions: map[logicalPos]value where 0=S, 1=P, 2=O
// VariablePositions: map[logicalPos]varName for joining
type RelationPattern struct {
	Prefix            byte
	BoundPositions    map[int]uint64
	VariablePositions map[int]string
}

// ErrTooManyVisits is returned when the LFTJ join exceeds the configured
// node visit limit. This protects against OOM during broad scans.
var ErrTooManyVisits = errors.New("query exceeded maximum node visits")

// visitCounterKey is the context key for the shared visit counter.
type visitCounterKey struct{}

// WithVisitCounter stores a shared visit counter and max in context.
func WithVisitCounter(ctx context.Context, counter *atomic.Int64, maxVisits int64) context.Context {
	ctx = context.WithValue(ctx, visitCounterKey{}, counter)
	ctx = context.WithValue(ctx, visitMaxKey{}, maxVisits)
	return ctx
}

type visitMaxKey struct{}

func visitCounterFromCtx(ctx context.Context) (*atomic.Int64, int64) {
	if v := ctx.Value(visitCounterKey{}); v != nil {
		counter := v.(*atomic.Int64)
		max := ctx.Value(visitMaxKey{}).(int64)
		return counter, max
	}
	return nil, 0
}

// LFTJEngine provides worst-case optimal multi-way joins with constant memory.
// Unlike nested-loop joins that materialize intermediate results,
// LFTJ traverses all relations simultaneously via trie iterators,
// yielding results one-by-one at the deepest level.
type LFTJEngine struct {
	db *badger.DB
}

func NewLFTJEngine(db *badger.DB) *LFTJEngine {
	return &LFTJEngine{db: db}
}

// Execute runs a multi-way join query. A shared *atomic.Int64 in ctx limits
// total BadgerDB operations (protects against runaway joins).
func (e *LFTJEngine) Execute(
	ctx context.Context,
	relations []RelationPattern,
	boundVars map[string]uint64,
	resultVars []string,
) iter.Seq2[map[string]uint64, error] {
	return func(yield func(map[string]uint64, error) bool) {
		if len(relations) == 0 {
			return
		}

		txn := e.db.NewTransaction(false)
		defer txn.Discard()

		vc, maxVisits := visitCounterFromCtx(ctx)

		iterators := make([]*TrieIterator, len(relations))
		for i, rel := range relations {
			iterators[i] = NewTrieIterator(txn, rel.Prefix, rel.BoundPositions)
		}

		// Collect all unbound variable positions across relations
		allVarPositions := make(map[string][]varOccurrence)
		for relIdx, rel := range relations {
			for pos, varName := range rel.VariablePositions {
				if _, isBound := rel.BoundPositions[pos]; isBound {
					continue
				}
				allVarPositions[varName] = append(allVarPositions[varName], varOccurrence{
					relIdx: relIdx,
					pos:    pos,
				})
			}
		}

		// Determine join order from resultVars
		joinVars := make([]string, 0, len(resultVars))
		for _, v := range resultVars {
			if occs, ok := allVarPositions[v]; ok && len(occs) > 0 {
				joinVars = append(joinVars, v)
			}
		}

		// Build initial bindings from bound vars
		bindings := make(map[string]uint64)
		for k, v := range boundVars {
			bindings[k] = v
		}

		// Leapfrog recursive join
		e.leapfrogRecursive(iterators, relations, allVarPositions, joinVars, 0, bindings, resultVars, vc, maxVisits, yield)
	}
}

type varOccurrence struct {
	relIdx int
	pos    int
}

func (e *LFTJEngine) leapfrogRecursive(
	iterators []*TrieIterator,
	relations []RelationPattern,
	varPositions map[string][]varOccurrence,
	joinVars []string,
	depth int,
	bindings map[string]uint64,
	resultVars []string,
	vc *atomic.Int64,
	maxVisits int64,
	yield func(map[string]uint64, error) bool,
) bool {
	if depth >= len(joinVars) {
		result := make(map[string]uint64, len(resultVars))
		for _, v := range resultVars {
			if val, ok := bindings[v]; ok {
				result[v] = val
			}
		}
		return yield(result, nil)
	}

	varName := joinVars[depth]
	occs := varPositions[varName]

	if len(occs) == 0 {
		return e.leapfrogRecursive(iterators, relations, varPositions, joinVars, depth+1, bindings, resultVars, vc, maxVisits, yield)
	}

	firstOcc := occs[0]
	it := iterators[firstOcc.relIdx]

	// Apply current bindings to iterator
	for _, occ := range occs {
		rel := relations[occ.relIdx]
		for pos, val := range bindings {
			for varPos, varName2 := range rel.VariablePositions {
				if varName2 == pos {
					it.SetBinding(varPos, val)
				}
			}
		}
	}

	// Open and iterate
	it.Open()
	defer it.Close()

	for {
		if it.AtEnd() {
			break
		}

		// Check visit limit
		if vc != nil && maxVisits > 0 {
			if vc.Add(1) > maxVisits {
				yield(nil, ErrTooManyVisits)
				return false
			}
		}

		value := it.Key()

		// Check consistency across all occurrences
		consistent := true
		for _, occ := range occs[1:] {
			otherIt := iterators[occ.relIdx]
			otherIt.Seek(value)
			if otherIt.AtEnd() || otherIt.Key() != value {
				consistent = false
				break
			}
		}

		if consistent {
			bindings[varName] = value
			if !e.leapfrogRecursive(iterators, relations, varPositions, joinVars, depth+1, bindings, resultVars, vc, maxVisits, yield) {
				return false
			}
		}

		// Leapfrog: advance to next candidate
		maxVal := value
		for _, occ := range occs {
			iterators[occ.relIdx].Next()
			if !iterators[occ.relIdx].AtEnd() {
				k := iterators[occ.relIdx].Key()
				if k > maxVal {
					maxVal = k
				}
			}
		}

		// Seek all to maxVal
		for _, occ := range occs {
			iterators[occ.relIdx].Seek(maxVal)
			if iterators[occ.relIdx].AtEnd() {
				return true
			}
		}
	}

	return true
}

// TrieIterator provides trie-structured access to a triple relation stored in BadgerDB.
type TrieIterator struct {
	txn    *badger.Txn
	it     *badger.Iterator
	prefix byte
	bound  map[int]uint64
	keyBuf []byte
	atEnd  bool
}

func NewTrieIterator(txn *badger.Txn, prefix byte, bound map[int]uint64) *TrieIterator {
	return &TrieIterator{
		txn:    txn,
		prefix: prefix,
		bound:  bound,
		keyBuf: make([]byte, keys.TripleKeySize),
		atEnd:  true,
	}
}

func (ti *TrieIterator) Open() {
	ti.it = ti.txn.NewIterator(badger.DefaultIteratorOptions)
	ti.buildPrefix()
	ti.it.Seek(ti.keyBuf[:ti.prefixLen()])
	ti.validatePosition()
}

func (ti *TrieIterator) Close() {
	if ti.it != nil {
		ti.it.Close()
		ti.it = nil
	}
}

func (ti *TrieIterator) SetBinding(pos int, val uint64) {
	if ti.bound == nil {
		ti.bound = make(map[int]uint64)
	}
	ti.bound[pos] = val
}

func (ti *TrieIterator) AtEnd() bool {
	return ti.atEnd
}

func (ti *TrieIterator) Key() uint64 {
	if ti.atEnd || ti.it == nil || !ti.it.Valid() {
		return 0
	}
	key := ti.it.Item().Key()
	if len(key) < keys.TripleKeySize {
		return 0
	}
	_, _, o := keys.DecodeTripleKey(key)
	return o
}

func (ti *TrieIterator) Seek(target uint64) {
	if ti.atEnd || ti.it == nil {
		return
	}

	boundWithTarget := make(map[int]uint64)
	for k, v := range ti.bound {
		boundWithTarget[k] = v
	}
	boundWithTarget[2] = target

	ti.keyBuf = keys.EncodeTripleKey(ti.prefix, boundWithTarget[0], boundWithTarget[1], boundWithTarget[2])
	ti.it.Seek(ti.keyBuf[:ti.prefixLen()+8])
	ti.validatePosition()
}

func (ti *TrieIterator) Next() {
	if ti.atEnd || ti.it == nil {
		return
	}
	ti.it.Next()
	ti.validatePosition()

	if !ti.atEnd && len(ti.bound) == 0 {
		ti.leapfrogTopicID()
	}
}

func (ti *TrieIterator) leapfrogTopicID() {
	if ti.it == nil || !ti.it.Valid() {
		return
	}

	key := ti.it.Item().Key()
	if len(key) < keys.TripleKeySize {
		return
	}

	s, _, _ := keys.DecodeTripleKey(key)
	currentTopic := keys.UnpackTopicID(s)
	expectedTopic := keys.UnpackTopicID(ti.bound[0])

	if currentTopic == expectedTopic {
		return
	}

	nextTopicID := keys.NextTopicID(s)
	seekKey := keys.EncodeTripleKey(ti.prefix, nextTopicID, 0, 0)
	ti.it.Seek(seekKey)
	ti.validatePosition()
}

func (ti *TrieIterator) prefixLen() int {
	return 1 + len(ti.bound)*8
}

func (ti *TrieIterator) buildPrefix() {
	s := ti.bound[0]
	p := ti.bound[1]
	o := ti.bound[2]
	ti.keyBuf = keys.EncodeTripleKey(ti.prefix, s, p, o)
}

func (ti *TrieIterator) validatePosition() {
	if ti.it == nil {
		ti.atEnd = true
		return
	}

	pLen := ti.prefixLen()
	for {
		if !ti.it.Valid() {
			ti.atEnd = true
			return
		}

		key := ti.it.Item().Key()
		if len(key) < keys.TripleKeySize {
			ti.it.Next()
			continue
		}

		match := true
		for i := 0; i < pLen && i < len(key); i++ {
			if key[i] != ti.keyBuf[i] {
				match = false
				break
			}
		}

		if match {
			ti.atEnd = false
			return
		}

		ti.it.Next()
	}
}
