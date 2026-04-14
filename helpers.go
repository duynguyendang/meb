package meb

import (
	"fmt"

	"github.com/duynguyendang/meb/keys"
)

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

// isInlineType returns true if the object type should be encoded as an inline ID.
func isInlineType(obj any) bool {
	switch obj.(type) {
	case bool, int32, float32:
		return true
	}
	return false
}

// encodeObject returns the string representation and dictionary ID for an object.
// For inline types (bool, int32, float32), returns an inline ID with bit 39 set.
// For string types, returns oID=0 (caller uses batch dict lookup).
// For other types (int, int64, float64), uses dictionary encoding.
func (m *MEBStore) encodeObject(obj any) (string, uint64, error) {
	if obj == nil {
		return "", 0, fmt.Errorf("%w: object cannot be nil", ErrInvalidFact)
	}

	switch v := obj.(type) {
	case string:
		return v, 0, nil
	case bool:
		return "", keys.PackInlineBool(v), nil
	case int32:
		return "", keys.PackInlineInt32(v), nil
	case float32:
		return "", keys.PackInlineFloat32(v), nil
	case int:
		// int goes to dictionary (common case, preserves exact string form)
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
		// Use %.17g to preserve full float64 precision (up to 17 significant digits)
		objStr := fmt.Sprintf("%.17g", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to encode float64 object: %w", err)
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
