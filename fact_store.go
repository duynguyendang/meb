package meb

import (
	"fmt"

	"github.com/google/mangle/ast"
	"github.com/google/mangle/factstore"
)

// extractQuadFromAtom extracts subject, predicate, object, and graph from an atom.
// For "triples" predicate, uses args: triples(S, P, O, G)
// For other predicates, uses atom.Predicate and args: predicate(S, O)
func extractQuadFromAtom(atom ast.Atom) (subject, predicate, object, graph string) {
	graph = "default"

	if atom.Predicate.Symbol == "triples" && len(atom.Args) >= 3 {
		if constTerm, ok := atom.Args[0].(ast.Constant); ok {
			subject = constTerm.Symbol
		}
		if constTerm, ok := atom.Args[1].(ast.Constant); ok {
			predicate = constTerm.Symbol
		}
		if constTerm, ok := atom.Args[2].(ast.Constant); ok {
			object = constTerm.Symbol
		}
		if len(atom.Args) > 3 {
			if constTerm, ok := atom.Args[3].(ast.Constant); ok {
				graph = constTerm.Symbol
			}
		}
	} else {
		predicate = atom.Predicate.Symbol
		if len(atom.Args) >= 2 {
			if constTerm, ok := atom.Args[0].(ast.Constant); ok {
				subject = constTerm.Symbol
			}
			if constTerm, ok := atom.Args[1].(ast.Constant); ok {
				object = constTerm.Symbol
			}
		}
	}

	return
}

// === factstore.FactStore implementation ===

// GetFacts streams facts matching the given atom using the callback.
// It implements streaming semantics - never loads all results into memory.
func (m *MEBStore) GetFacts(atom ast.Atom, callback func(ast.Atom) error) error {
	s, p, o, g := extractQuadFromAtom(atom)

	// Note: Graph is not currently supported in the Datalog API

	// Use Scan to find matching facts
	for fact, err := range m.Scan(s, p, o, g) {
		if err != nil {
			return err
		}

		// Convert Fact back to ast.Atom format
		resultArgs := make([]ast.BaseTerm, 3)
		resultArgs[0] = ast.Constant{Type: ast.StringType, Symbol: fact.Subject}
		resultArgs[1] = ast.Constant{Type: ast.StringType, Symbol: fact.Predicate}

		// Object is stored as string in dictionary
		objectStr, ok := fact.Object.(string)
		if !ok {
			objectStr = fmt.Sprintf("%v", fact.Object)
		}
		resultArgs[2] = ast.Constant{Type: ast.StringType, Symbol: objectStr}

		resultAtom := ast.Atom{
			Predicate: atom.Predicate,
			Args:      resultArgs,
		}

		if err := callback(resultAtom); err != nil {
			return err
		}
	}

	return nil
}

// Add adds a fact to the store and returns true if it didn't exist before.
func (m *MEBStore) Add(atom ast.Atom) bool {
	// Check if already exists
	if m.Contains(atom) {
		return false
	}

	subject, predicate, object, graph := extractQuadFromAtom(atom)

	// Add fact using the quad store format
	fact := Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Graph:     graph,
	}

	err := m.AddFactBatch([]Fact{fact})
	if err != nil {
		return false
	}

	return true
}

// Contains returns true if the given atom is present in the store.
func (m *MEBStore) Contains(atom ast.Atom) bool {
	found := false
	m.GetFacts(atom, func(ast.Atom) error {
		found = true
		// Return error to stop iteration
		return fmt.Errorf("found")
	})

	return found
}

// ListPredicates lists all predicates available in the store.
func (m *MEBStore) ListPredicates() []ast.PredicateSym {
	m.mu.RLock()
	defer m.mu.RUnlock()

	preds := make([]ast.PredicateSym, 0, len(m.predicates))
	for pred := range m.predicates {
		preds = append(preds, pred)
	}
	return preds
}

// Merge merges contents of the given store into this store.
func (m *MEBStore) Merge(other factstore.ReadOnlyFactStore) error {
	for _, pred := range other.ListPredicates() {
		if err := other.GetFacts(ast.NewQuery(pred), func(atom ast.Atom) error {
			m.Add(atom)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
