package meb

import (
	"fmt"
	"sync"
	"time"

	"codeberg.org/TauCeti/mangle-go/ast"
	"codeberg.org/TauCeti/mangle-go/factstore"
)

// TemporalFact extends Fact with time validity interval.
// This enables temporal queries like "who accessed doc_X in the last 30 days?"
type TemporalFact struct {
	Fact
	ValidFrom time.Time
	ValidTo   time.Time
}

// MEBTemporalStore implements factstore.TemporalFactStore for MEB.
// It stores temporal facts with time intervals and supports temporal queries.
type MEBTemporalStore struct {
	mu sync.RWMutex
	// Map of predicate -> slice of temporal facts
	facts map[ast.PredicateSym][]temporalFactEntry
}

type temporalFactEntry struct {
	atom     ast.Atom
	interval ast.Interval
}

// NewMEBTemporalStore creates a new temporal store for MEB.
func NewMEBTemporalStore() *MEBTemporalStore {
	return &MEBTemporalStore{
		facts: make(map[ast.PredicateSym][]temporalFactEntry),
	}
}

// Add adds a temporal fact with the given interval.
func (m *MEBTemporalStore) Add(atom ast.Atom, interval ast.Interval) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := m.facts[atom.Predicate]
	for _, e := range entries {
		if e.atom.Equals(atom) && e.interval.Equals(interval) {
			return false, nil // Already exists
		}
	}

	m.facts[atom.Predicate] = append(entries, temporalFactEntry{
		atom:     atom,
		interval: interval,
	})
	return true, nil
}

// AddEternal adds a fact that's valid for all time.
func (m *MEBTemporalStore) AddEternal(atom ast.Atom) (bool, error) {
	// Eternal interval - from negative infinity to positive infinity
	eternalInterval := ast.EternalInterval()
	return m.Add(atom, eternalInterval)
}

// ContainsAt checks if a fact is valid at the given time.
func (m *MEBTemporalStore) ContainsAt(atom ast.Atom, t time.Time) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, ok := m.facts[atom.Predicate]
	if !ok {
		return false
	}

	for _, e := range entries {
		if e.atom.Equals(atom) && e.interval.Contains(t) {
			return true
		}
	}
	return false
}

// GetFactsAt returns all facts matching the query that are valid at time t.
func (m *MEBTemporalStore) GetFactsAt(query ast.Atom, t time.Time, callback func(factstore.TemporalFact) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, ok := m.facts[query.Predicate]
	if !ok {
		return nil // No facts for this predicate
	}

	for _, e := range entries {
		if !e.interval.Contains(t) {
			continue
		}

		// Check if arguments match
		if len(query.Args) > 0 && len(e.atom.Args) != len(query.Args) {
			continue
		}

		// For ground queries, check exact match
		if query.IsGround() {
			if e.atom.Equals(query) {
				tf := factstore.TemporalFact{
					Atom:     e.atom,
					Interval: e.interval,
				}
				if err := callback(tf); err != nil {
					return err
				}
			}
			continue
		}

		// For non-ground queries, return all matching facts
		// (Simple implementation - doesn't do full unification)
		tf := factstore.TemporalFact{
			Atom:     e.atom,
			Interval: e.interval,
		}
		if err := callback(tf); err != nil {
			return err
		}
	}

	return nil
}

// GetFactsDuring returns all facts that are valid during the given interval.
func (m *MEBTemporalStore) GetFactsDuring(query ast.Atom, interval ast.Interval, callback func(factstore.TemporalFact) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, ok := m.facts[query.Predicate]
	if !ok {
		return nil
	}

	for _, e := range entries {
		if !e.interval.Overlaps(interval) {
			continue
		}

		tf := factstore.TemporalFact{
			Atom:     e.atom,
			Interval: e.interval,
		}
		if err := callback(tf); err != nil {
			return err
		}
	}

	return nil
}

// GetAllFacts returns all temporal facts matching the query (regardless of time).
func (m *MEBTemporalStore) GetAllFacts(query ast.Atom, callback func(factstore.TemporalFact) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, ok := m.facts[query.Predicate]
	if !ok {
		return nil
	}

	for _, e := range entries {
		tf := factstore.TemporalFact{
			Atom:     e.atom,
			Interval: e.interval,
		}
		if err := callback(tf); err != nil {
			return err
		}
	}

	return nil
}

// ListPredicates returns all predicates in the store.
func (m *MEBTemporalStore) ListPredicates() []ast.PredicateSym {
	m.mu.RLock()
	defer m.mu.RUnlock()

	preds := make([]ast.PredicateSym, 0, len(m.facts))
	for pred := range m.facts {
		preds = append(preds, pred)
	}
	return preds
}

// EstimateFactCount returns the total number of temporal facts.
func (m *MEBTemporalStore) EstimateFactCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, entries := range m.facts {
		count += len(entries)
	}
	return count
}

// Coalesce merges overlapping intervals for a given predicate.
func (m *MEBTemporalStore) Coalesce(predicate ast.PredicateSym) error {
	// Implementation for coalescing overlapping intervals
	// This is an advanced feature that can be added later
	return nil
}

// Merge merges another temporal store into this one.
func (m *MEBTemporalStore) Merge(other factstore.ReadOnlyTemporalFactStore) error {
	otherPreds := other.ListPredicates()
	for _, pred := range otherPreds {
		err := other.GetAllFacts(ast.NewQuery(pred), func(tf factstore.TemporalFact) error {
			_, err := m.Add(tf.Atom, tf.Interval)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to merge temporal facts: %w", err)
		}
	}
	return nil
}

// GetTemporalFactCount returns the number of facts valid at a specific time.
func (m *MEBTemporalStore) GetTemporalFactCount(t time.Time) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, entries := range m.facts {
		for _, e := range entries {
			if e.interval.Contains(t) {
				count++
			}
		}
	}
	return count
}

// Clear removes all temporal facts from the store.
func (m *MEBTemporalStore) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.facts = make(map[ast.PredicateSym][]temporalFactEntry)
}
