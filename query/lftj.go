// Package query implements Leapfrog Triejoin (LFTJ) for worst-case optimal multi-way joins
package query

import (
	"context"
	"fmt"
	"iter"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

// TrieIterator provides trie-structured access to a relation stored in BadgerDB.
// It views the key-value store as a prefix tree (trie) where tuples are sorted lexicographically.
type TrieIterator struct {
	txn    *badger.Txn
	it     *badger.Iterator
	prefix []byte // Current position prefix
	depth  int    // Current trie depth (0=first ID, 1=second ID, etc.)

	// columnOrder defines the attribute positions for this iterator
	// For quad SPOG: [0,1,2,3] = Subject, Predicate, Object, Graph
	columnOrder []int

	// keySize is the expected key size (33 for quad keys)
	keySize int

	// exhausted tracks if iterator has reached end
	exhausted bool
}

// NewTrieIterator creates a new TrieIterator for a given prefix and column order.
// prefix: the initial prefix to seek to (e.g., QuadSPOGPrefix for SPOG index)
// columnOrder: the attribute ordering (e.g., [0,1,2,3] for S-P-O-G)
func NewTrieIterator(txn *badger.Txn, prefix []byte, columnOrder []int) *TrieIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // We only need keys

	it := txn.NewIterator(opts)

	return &TrieIterator{
		txn:         txn,
		it:          it,
		prefix:      prefix,
		depth:       0,
		columnOrder: columnOrder,
		keySize:     keys.QuadKeySize,
		exhausted:   false,
	}
}

// Open initializes the iterator at the root of the trie.
// Seeks to the initial prefix position.
func (ti *TrieIterator) Open() error {
	if ti.it == nil {
		return fmt.Errorf("iterator not initialized")
	}

	ti.it.Seek(ti.prefix)
	if !ti.it.ValidForPrefix(ti.prefix) {
		ti.exhausted = true
	}

	return nil
}

// Close releases the iterator resources.
func (ti *TrieIterator) Close() {
	if ti.it != nil {
		ti.it.Close()
	}
}

// Up moves up to the parent level in the trie (decreases depth).
func (ti *TrieIterator) Up() {
	if ti.depth > 0 {
		ti.depth--
	}
}

// Down moves down to a child level in the trie (increases depth).
func (ti *TrieIterator) Down() {
	ti.depth++
}

// Seek seeks to the specified key at the current depth.
// Returns true if found, false if not found or exhausted.
func (ti *TrieIterator) Seek(target uint64) bool {
	if ti.exhausted {
		return false
	}

	// Build seek key: prefix + target at current depth position
	seekPrefix := make([]byte, len(ti.prefix)+ti.depth*8+8)
	copy(seekPrefix, ti.prefix)

	// Copy existing depth components
	if ti.depth > 0 && ti.it.Valid() {
		key := ti.it.Item().Key()
		if len(key) >= len(ti.prefix)+ti.depth*8 {
			copy(seekPrefix[len(ti.prefix):], key[len(ti.prefix):len(ti.prefix)+ti.depth*8])
		}
	}

	// Set target at current depth
	seekPrefix[len(ti.prefix)+ti.depth*8] = byte(target >> 56)
	seekPrefix[len(ti.prefix)+ti.depth*8+1] = byte(target >> 48)
	seekPrefix[len(ti.prefix)+ti.depth*8+2] = byte(target >> 40)
	seekPrefix[len(ti.prefix)+ti.depth*8+3] = byte(target >> 32)
	seekPrefix[len(ti.prefix)+ti.depth*8+4] = byte(target >> 24)
	seekPrefix[len(ti.prefix)+ti.depth*8+5] = byte(target >> 16)
	seekPrefix[len(ti.prefix)+ti.depth*8+6] = byte(target >> 8)
	seekPrefix[len(ti.prefix)+ti.depth*8+7] = byte(target)

	ti.it.Seek(seekPrefix)

	// Check if we're still within the prefix
	if !ti.it.ValidForPrefix(ti.prefix) {
		ti.exhausted = true
		return false
	}

	// Get current key and check if we're at or after target
	key := ti.it.Item().Key()
	if len(key) < len(ti.prefix)+(ti.depth+1)*8 {
		return false
	}

	currentVal := extractUint64(key, len(ti.prefix)+ti.depth*8)
	return currentVal >= target
}

// Next moves to the next sibling at the current depth.
// Returns true if successful, false if exhausted.
func (ti *TrieIterator) Next() bool {
	if ti.exhausted {
		return false
	}

	ti.it.Next()

	if !ti.it.ValidForPrefix(ti.prefix) {
		ti.exhausted = true
		return false
	}

	return true
}

// Key returns the current key component at the specified depth.
func (ti *TrieIterator) Key() uint64 {
	if ti.exhausted || !ti.it.Valid() {
		return 0
	}

	key := ti.it.Item().Key()
	if len(key) < len(ti.prefix)+(ti.depth+1)*8 {
		return 0
	}

	return extractUint64(key, len(ti.prefix)+ti.depth*8)
}

// FullKey returns the complete key at current position.
func (ti *TrieIterator) FullKey() []byte {
	if ti.exhausted || !ti.it.Valid() {
		return nil
	}
	return ti.it.Item().Key()
}

// AtEnd returns true if the iterator is exhausted.
func (ti *TrieIterator) AtEnd() bool {
	return ti.exhausted
}

// extractUint64 extracts a uint64 from a byte slice at the given offset.
func extractUint64(b []byte, offset int) uint64 {
	if len(b) < offset+8 {
		return 0
	}
	return uint64(b[offset])<<56 |
		uint64(b[offset+1])<<48 |
		uint64(b[offset+2])<<40 |
		uint64(b[offset+3])<<32 |
		uint64(b[offset+4])<<24 |
		uint64(b[offset+5])<<16 |
		uint64(b[offset+6])<<8 |
		uint64(b[offset+7])
}

// iteratorInfo holds iterator state for leapfrog join
type iteratorInfo struct {
	iter  *TrieIterator
	key   uint64
	index int
}

// LeapfrogJoin performs a multi-way leapfrog join on the given iterators.
// It finds the intersection of all iterators at each trie level efficiently.
// Returns an iter.Seq2 that yields joined tuples as []uint64.
func LeapfrogJoin(iterators []*TrieIterator) iter.Seq2[[]uint64, error] {
	return func(yield func([]uint64, error) bool) {
		if len(iterators) == 0 {
			return
		}

		// Initialize all iterators
		for _, it := range iterators {
			if err := it.Open(); err != nil {
				yield(nil, fmt.Errorf("failed to open iterator: %w", err))
				return
			}
			defer it.Close()
		}

		// Perform leapfrog join
		if err := leapfrogRecursive(iterators, []uint64{}, yield); err != nil {
			yield(nil, err)
		}
	}
}

// leapfrogRecursive performs the recursive leapfrog join algorithm.
func leapfrogRecursive(iterators []*TrieIterator, currentTuple []uint64, yield func([]uint64, error) bool) error {
	if len(iterators) == 0 {
		// All iterators exhausted, yield current tuple
		if !yield(append([]uint64(nil), currentTuple...), nil) {
			return context.Canceled
		}
		return nil
	}

	// Get current keys from all iterators
	infos := make([]iteratorInfo, 0, len(iterators))
	for i, it := range iterators {
		if !it.AtEnd() {
			infos = append(infos, iteratorInfo{
				iter:  it,
				key:   it.Key(),
				index: i,
			})
		}
	}

	// If any iterator is exhausted, we're done at this level
	if len(infos) < len(iterators) {
		return nil
	}

	// Sort by current key
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].key < infos[j].key
	})

	// Leapfrog algorithm
	maxKey := infos[len(infos)-1].key
	numIterators := len(infos)

	for {
		// Check if all iterators are at the same key
		if infos[0].key == maxKey {
			// Match found! Recurse to next level
			matchKey := infos[0].key

			// Move all iterators down
			for _, info := range infos {
				info.iter.Down()
			}

			// Recurse with the match
			if err := leapfrogRecursive(
				collectIterators(iterators),
				append(currentTuple, matchKey),
				yield,
			); err != nil {
				return err
			}

			// Move all iterators back up and advance
			for _, info := range infos {
				info.iter.Up()
				info.iter.Next()
				if info.iter.AtEnd() {
					return nil // One iterator exhausted, done
				}
			}

			// Rebuild infos with new keys
			infos = infos[:0]
			for i, it := range iterators {
				if !it.AtEnd() {
					infos = append(infos, iteratorInfo{
						iter:  it,
						key:   it.Key(),
						index: i,
					})
				}
			}
			if len(infos) < numIterators {
				return nil
			}
			sort.Slice(infos, func(i, j int) bool {
				return infos[i].key < infos[j].key
			})
			maxKey = infos[len(infos)-1].key
			continue
		}

		// Seek the iterator with minimum key to max_key
		minInfo := &infos[0]
		if !minInfo.iter.Seek(maxKey) {
			// Seek failed, iterator exhausted
			return nil
		}

		// Update the key after seek
		minInfo.key = minInfo.iter.Key()

		// If we advanced past max_key, update max_key
		if minInfo.key > maxKey {
			maxKey = minInfo.key
		}

		// Re-sort after seek
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].key < infos[j].key
		})
	}
}

// collectIterators returns the slice of iterators (helper for recursion)
func collectIterators(iterators []*TrieIterator) []*TrieIterator {
	result := make([]*TrieIterator, 0, len(iterators))
	for _, it := range iterators {
		if !it.AtEnd() {
			result = append(result, it)
		}
	}
	return result
}

// VariableOrder represents the optimal ordering of attributes for LFTJ
type VariableOrder struct {
	Attributes []Attribute // Ordered list of attributes to join
	Cost       float64     // Estimated cost
}

// Attribute represents a single attribute in the variable order
type Attribute struct {
	Position    int     // Position in tuple (0=subject, 1=predicate, 2=object, 3=graph)
	Relation    int     // Which relation (for multi-relation queries)
	Selectivity float64 // Estimated selectivity (lower = more selective)
}

// DetermineOptimalOrder uses heuristics to find the best variable ordering.
// Returns attributes ordered from most selective to least selective.
func DetermineOptimalOrder(boundPositions map[int]uint64, cardinalities map[int]int) VariableOrder {
	attributes := make([]Attribute, 0)

	// Add bound positions first (they are most selective)
	for pos := range boundPositions {
		attributes = append(attributes, Attribute{
			Position:    pos,
			Selectivity: 0.0, // Bound positions have perfect selectivity
		})
	}

	// Add remaining positions sorted by cardinality (lower = more selective)
	remaining := make([]Attribute, 0)
	for pos, card := range cardinalities {
		if _, isBound := boundPositions[pos]; !isBound {
			selectivity := float64(card)
			remaining = append(remaining, Attribute{
				Position:    pos,
				Selectivity: selectivity,
			})
		}
	}

	// Sort remaining by selectivity (ascending)
	sort.Slice(remaining, func(i, j int) bool {
		return remaining[i].Selectivity < remaining[j].Selectivity
	})

	attributes = append(attributes, remaining...)

	// Calculate total cost
	cost := 0.0
	for _, attr := range attributes {
		cost += attr.Selectivity
	}

	return VariableOrder{
		Attributes: attributes,
		Cost:       cost,
	}
}
