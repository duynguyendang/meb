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

// stringRef records which element of a deduped string slice a fact component
// maps to. isObj distinguishes string objects (dict-backed, packed) from
// subject/predicate positions (also dict-backed, but never inline-packed).
type stringRef struct {
	index int
	isObj bool
}

// collectStringRefs walks a fact set once, deduplicating the strings that need
// dictionary ids. It returns per-fact refs (subject, predicate, optional string
// object) and the ordered slice of unique strings for a single GetIDs call.
func collectStringRefs(facts []Fact) (refs [][]stringRef, unique []string) {
	refs = make([][]stringRef, len(facts))
	idx := make(map[string]int)
	add := func(s string) int {
		if i, ok := idx[s]; ok {
			return i
		}
		idx[s] = len(unique)
		unique = append(unique, s)
		return len(unique) - 1
	}
	for i, fact := range facts {
		refs[i] = append(refs[i], stringRef{index: add(fact.Subject)})
		refs[i] = append(refs[i], stringRef{index: add(fact.Predicate)})
		if s, ok := fact.Object.(string); ok {
			refs[i] = append(refs[i], stringRef{index: add(s), isObj: true})
		}
	}
	return refs, unique
}

// factKey holds the dual-index key pair and shared value for a single fact,
// plus a dedup key (the SPO key bytes) used by bulk writers to skip re-inserts.
type factKey struct {
	spo, ops, value []byte
	dedup           string
}

// encodeFactKeys maps each fact to its topic-packed SPO/OPS keys and shared
// value using the pre-resolved dictionary ids (from collectStringRefs + GetIDs).
func (m *MEBStore) encodeFactKeys(facts []Fact, factRefs [][]stringRef, ids []uint64) ([]factKey, error) {
	fks := make([]factKey, len(facts))
	topic := m.topicID.Load()
	for i, fact := range facts {
		sID := ids[factRefs[i][0].index]
		pID := ids[factRefs[i][1].index]

		var oID uint64
		var isInline bool
		if len(factRefs[i]) > 2 && factRefs[i][2].isObj {
			oID = ids[factRefs[i][2].index]
		} else {
			_, eoID, err := m.encodeObject(fact.Object)
			if err != nil {
				return nil, fmt.Errorf("failed to encode object for fact %d: %w", i, err)
			}
			oID = eoID
			isInline = keys.IsInline(oID)
		}

		sID = keys.PackID(topic, keys.UnpackLocalID(sID))
		if !isInline {
			oID = keys.PackID(topic, keys.UnpackLocalID(oID))
		}

		hints := keys.EncodeSemanticHints(m.defaultEntityType, uint16(keys.HashSemanticName(fact.Subject)), m.defaultFlags)
		value := encodeTripleValueWithHints(0, 0, hints)

		spoKey := keys.EncodeTripleKey(keys.TripleSPOPrefix, sID, pID, oID)
		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, sID, pID, oID)
		fks[i] = factKey{spo: spoKey, ops: opsKey, value: value, dedup: string(spoKey)}
	}
	return fks, nil
}

// pow2Ceil returns the smallest power of two that is >= n.
func pow2Ceil(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
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
