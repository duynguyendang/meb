package meb

import (
	"fmt"
	"strings"

	"github.com/google/mangle/ast"
)

// === Validation Functions ===

// validateFact checks if a fact has valid fields.
func validateFact(fact Fact) error {
	if fact.Subject == "" {
		return fmt.Errorf("%w: subject cannot be empty", ErrInvalidFact)
	}
	if fact.Predicate == "" {
		return fmt.Errorf("%w: predicate cannot be empty", ErrInvalidFact)
	}
	if fact.Object == nil {
		return fmt.Errorf("%w: object cannot be nil", ErrInvalidFact)
	}
	return nil
}

// validateFacts validates a batch of facts.
func validateFacts(facts []Fact) error {
	if len(facts) == 0 {
		return ErrEmptyBatch
	}
	for i, fact := range facts {
		if err := validateFact(fact); err != nil {
			return fmt.Errorf("fact at index %d: %w", i, err)
		}
	}
	return nil
}

// === Normalization Functions ===

// normalizeGraph returns "default" if graph is empty, otherwise returns the input.
func normalizeGraph(graph string) string {
	if graph == "" {
		return "default"
	}
	return graph
}

// === ID Resolution Functions ===

// resolveStringToID converts a string to its dictionary ID.
// Returns error if string not found.
func (m *MEBStore) resolveStringToID(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("cannot resolve empty string to ID")
	}
	id, err := m.dict.GetID(s)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve '%s' to ID: %w", s, err)
	}
	return id, nil
}

// === Encoding Functions ===

// encodeObject converts an object value to its dictionary ID and string representation.
// Handles various types including int, int64, float64, bool, and string.
func (m *MEBStore) encodeObject(obj any) (string, uint64, error) {
	if obj == nil {
		return "", 0, fmt.Errorf("%w: object cannot be nil", ErrInvalidFact)
	}

	switch v := obj.(type) {
	case string:
		return v, 0, nil // ID will be obtained from batch
	case int:
		objStr := fmt.Sprintf("%d", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to encode int object: %w", err)
		}
		return objStr, oID, nil
	case int64:
		objStr := fmt.Sprintf("%d", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to encode int64 object: %w", err)
		}
		return objStr, oID, nil
	case float64:
		objStr := fmt.Sprintf("%f", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to encode float64 object: %w", err)
		}
		return objStr, oID, nil
	case bool:
		objStr := fmt.Sprintf("%t", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to encode bool object: %w", err)
		}
		return objStr, oID, nil
	default:
		objStr := fmt.Sprintf("%v", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to encode object of type %T: %w", v, err)
		}
		return objStr, oID, nil
	}
}

// === Mangle Integration Functions ===

// factToAtom converts a Fact to a Mangle Atom.
func (m *MEBStore) factToAtom(fact Fact) (ast.Atom, error) {
	// Create args from Subject, Object, and Graph (not Predicate)
	args := []ast.BaseTerm{
		ast.String(fact.Subject),
		m.goValueToTerm(fact.Object),
		ast.String(fact.Graph),
	}
	return ast.NewAtom(fact.Predicate, args...), nil
}

// goValueToTerm converts a Go value to a Mangle term.
func (m *MEBStore) goValueToTerm(v any) ast.BaseTerm {
	switch val := v.(type) {
	case string:
		return ast.String(val)
	case int:
		return ast.Number(int64(val))
	case int64:
		return ast.Number(val)
	case float64:
		return ast.Float64(val)
	case bool:
		if val {
			return ast.String("true")
		}
		return ast.String("false")
	default:
		return ast.String(fmt.Sprintf("%v", val))
	}
}

// termToGoValue converts a Mangle term to a Go value.
func (m *MEBStore) termToGoValue(term ast.BaseTerm) any {
	switch t := term.(type) {
	case ast.Constant:
		switch t.Type {
		case ast.StringType:
			return t.Symbol
		case ast.NumberType:
			return t.NumValue
		case ast.Float64Type:
			return t.NumValue
		default:
			return t.Symbol
		}
	default:
		return fmt.Sprintf("%v", t)
	}
}

// === Query Parsing Functions ===

// ParsedAtom represents a single atom in the query (either data or constraint).
type ParsedAtom struct {
	Predicate string
	Args      []string
}

// parseQuery parses a Datalog query string which may contain multiple atoms.
// Returns a list of all atoms found in the query.
func parseQuery(query string) ([]ParsedAtom, error) {
	query = strings.TrimSpace(query)

	// Remove leading ? from the very beginning
	query = strings.TrimPrefix(query, "?")

	// Use smart splitter to get top-level atoms
	atoms := SmartSplit(query)
	if len(atoms) == 0 {
		return nil, fmt.Errorf("%w: empty query", ErrInvalidQuery)
	}

	var parsedAtoms []ParsedAtom
	for _, atomStr := range atoms {
		// syntactic sugar: "A != B" -> neq(A, B)
		if strings.Contains(atomStr, "!=") {
			parts := strings.Split(atomStr, "!=")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid inequality format: %s", atomStr)
			}
			lhs := strings.TrimSpace(parts[0])
			rhs := strings.TrimSpace(parts[1])
			parsedAtoms = append(parsedAtoms, ParsedAtom{
				Predicate: "neq",
				Args:      []string{lhs, rhs},
			})
			continue
		}

		pred, args, err := parseAtom(atomStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse atom '%s': %w", atomStr, err)
		}
		parsedAtoms = append(parsedAtoms, ParsedAtom{
			Predicate: pred,
			Args:      args,
		})
	}

	return parsedAtoms, nil
}

// parseAtom parses a single atom string like "predicate(arg1, arg2)"
func parseAtom(atom string) (string, []string, error) {
	atom = strings.TrimSpace(atom)
	start := strings.Index(atom, "(")
	end := strings.LastIndex(atom, ")")

	if start == -1 || end == -1 || start >= end {
		return "", nil, fmt.Errorf("%w: expected format 'predicate(args...)' but got '%s'", ErrInvalidQuery, atom)
	}

	predicate := strings.TrimSpace(atom[:start])
	argsStr := strings.TrimSpace(atom[start+1 : end])

	var args []string
	if argsStr != "" {
		args = SmartSplit(argsStr)
	}
	return predicate, args, nil
}

// SmartSplit splits a string by comma, correctly handling quotes and nested parentheses.
func SmartSplit(query string) []string {
	var results []string
	var current strings.Builder
	depth := 0
	inQuotes := false

	for _, r := range query {
		switch r {
		case '"':
			inQuotes = !inQuotes
		case '(':
			if !inQuotes {
				depth++
			}
		case ')':
			if !inQuotes {
				depth--
			}
		case ',':
			if !inQuotes && depth == 0 {
				results = append(results, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}
	results = append(results, strings.TrimSpace(current.String()))
	return results
}
