package query

import (
	"fmt"
	"strings"
)

// Atom represents a single unit in a Datalog query (e.g., triples(S, P, O) or neq(A, B)).
type Atom struct {
	Predicate string   `json:"predicate"`
	Args      []string `json:"args"`
}

// ParseDatalog parses a Datalog query string into individual atoms.
// It supports standard predicates ('triples'), constraints ('regex', 'contains', 'not'),
// and syntactic sugar ('!=', '>=', '<=', '>', '<', '=').
func ParseDatalog(query string) ([]Atom, error) {
	query = strings.TrimSpace(query)
	// Handle "Head :- Body" syntax by taking Body (ignore Head)
	if idx := strings.Index(query, ":-"); idx != -1 {
		query = query[idx+2:]
	}
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ".")
	query = strings.TrimPrefix(query, "?")

	rawAtoms := SmartSplit(query)
	if len(rawAtoms) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	var parsedAtoms []Atom
	for _, raw := range rawAtoms {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		// Handle syntactic sugar: A != B
		if strings.Contains(raw, "!=") {
			parts := strings.SplitN(raw, "!=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid inequality format: %s", raw)
			}
			lhs := strings.TrimSpace(parts[0])
			rhs := strings.TrimSpace(parts[1])
			parsedAtoms = append(parsedAtoms, Atom{
				Predicate: "neq",
				Args:      []string{lhs, cleanSugarArg(rhs)},
			})
			continue
		}

		// Handle comparison operators: A >= B, A <= B, A > B, A < B, A = B
		parsedComparison := false
		for _, comp := range []struct {
			op        string
			predicate string
		}{
			{">=", "gte"},
			{"<=", "lte"},
			{">", "gt"},
			{"<", "lt"},
			{"=", "eq"},
		} {
			if strings.Contains(raw, comp.op) && !strings.Contains(raw, "(") {
				parts := strings.SplitN(raw, comp.op, 2)
				if len(parts) == 2 {
					lhs := strings.TrimSpace(parts[0])
					rhs := strings.TrimSpace(parts[1])
					parsedAtoms = append(parsedAtoms, Atom{
						Predicate: comp.predicate,
						Args:      []string{lhs, cleanSugarArg(rhs)},
					})
					parsedComparison = true
					break
				}
			}
		}
		if parsedComparison {
			continue
		}

		// Standard atom: Predicate(Args...)
		pred, args, err := parseAtomString(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse atom '%s': %w", raw, err)
		}
		parsedAtoms = append(parsedAtoms, Atom{
			Predicate: pred,
			Args:      args,
		})
	}

	return parsedAtoms, nil
}

// cleanSugarArg strips surrounding quotes from a comparison literal.
func cleanSugarArg(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1]
	}
	return s
}

// parseAtomString parses "predicate(arg1, arg2, ...)"
func parseAtomString(s string) (string, []string, error) {
	s = strings.TrimSpace(s)
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")

	if start == -1 || end == -1 || start >= end {
		return "", nil, fmt.Errorf("expected format 'predicate(args...)' but got '%s'", s)
	}

	predicate := strings.TrimSpace(s[:start])
	argsBody := s[start+1 : end]

	args := SmartSplit(argsBody)
	cleanedArgs := make([]string, len(args))
	for i, arg := range args {
		cleaned := strings.TrimSpace(arg)
		cleaned = strings.Trim(cleaned, "\"'")
		cleanedArgs[i] = cleaned
	}

	return predicate, cleanedArgs, nil
}

// SmartSplit splits a string by comma, correctly handling quotes and nested parentheses.
func SmartSplit(s string) []string {
	var results []string
	var current strings.Builder
	depth := 0
	inQuote := false
	var quoteChar rune

	for i, r := range s {
		switch r {
		case '\\':
			if i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\'') {
				current.WriteRune(r)
			} else {
				current.WriteRune(r)
			}
		case '"', '\'':
			if inQuote {
				if r == quoteChar {
					inQuote = false
					current.WriteRune(r)
				} else {
					current.WriteRune(r)
				}
			} else {
				inQuote = true
				quoteChar = r
				current.WriteRune(r)
			}
		case '(':
			if !inQuote {
				depth++
			}
			current.WriteRune(r)
		case ')':
			if !inQuote {
				depth--
			}
			current.WriteRune(r)
		case ',':
			if !inQuote && depth == 0 {
				results = append(results, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
			current.WriteRune(r)
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		results = append(results, strings.TrimSpace(current.String()))
	}
	return results
}
