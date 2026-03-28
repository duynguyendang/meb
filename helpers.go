package meb

import (
	"fmt"
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

func (m *MEBStore) encodeObject(obj any) (string, uint64, error) {
	if obj == nil {
		return "", 0, fmt.Errorf("%w: object cannot be nil", ErrInvalidFact)
	}

	switch v := obj.(type) {
	case string:
		return v, 0, nil
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
