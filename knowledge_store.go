package meb

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

const TripleValueSize = 16

// encodeTripleValue encodes a triple value with vector ID and content offset.
// The upper 16 bits of contentOffset can carry semantic hints.
func encodeTripleValue(vectorID, contentOffset uint64) []byte {
	val := make([]byte, TripleValueSize)
	binary.BigEndian.PutUint64(val[0:8], vectorID)
	binary.BigEndian.PutUint64(val[8:16], contentOffset)
	return val
}

// encodeTripleValueWithHints encodes a triple value with semantic hints packed into
// the upper 16 bits of contentOffset. Lower 48 bits hold the actual offset.
func encodeTripleValueWithHints(vectorID uint64, contentOffset uint64, hints uint16) []byte {
	packed := (uint64(hints) << 48) | (contentOffset & ((1 << 48) - 1))
	val := make([]byte, TripleValueSize)
	binary.BigEndian.PutUint64(val[0:8], vectorID)
	binary.BigEndian.PutUint64(val[8:16], packed)
	return val
}

// ShouldPruneTriple reads semantic hints from a 16-byte triple value and returns true
// if the triple should be pruned based on the requested entity type and public flag.
func ShouldPruneTriple(value []byte, wantEntityType uint16, wantPublic bool) bool {
	if len(value) < 16 {
		return false
	}
	packed := binary.BigEndian.Uint64(value[8:16])
	hints := uint16(packed >> 48)
	entityType, _, flags := keys.DecodeSemanticHints(hints)

	if wantEntityType > 0 && entityType != wantEntityType {
		return true
	}
	if wantPublic && (flags&keys.FlagIsPublic) == 0 {
		return true
	}
	return false
}

func (m *MEBStore) AddFact(fact Fact) error {
	if err := validateFact(fact); err != nil {
		return fmt.Errorf("failed to add fact: %w", err)
	}
	return m.AddFactBatch([]Fact{fact})
}

func (m *MEBStore) AddFactBatch(facts []Fact) error {
	if err := validateFacts(facts); err != nil {
		return fmt.Errorf("batch validation failed: %w", err)
	}

	type stringRef struct {
		index int
		isObj bool
	}
	factStringRefs := make([][]stringRef, len(facts))
	uniqueStringsMap := make(map[string]int)
	var uniqueStrings []string

	for i, fact := range facts {
		if _, ok := uniqueStringsMap[fact.Subject]; !ok {
			uniqueStringsMap[fact.Subject] = len(uniqueStrings)
			uniqueStrings = append(uniqueStrings, fact.Subject)
		}
		factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[fact.Subject], isObj: false})

		if _, ok := uniqueStringsMap[fact.Predicate]; !ok {
			uniqueStringsMap[fact.Predicate] = len(uniqueStrings)
			uniqueStrings = append(uniqueStrings, fact.Predicate)
		}
		factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[fact.Predicate], isObj: false})

		if s, ok := fact.Object.(string); ok {
			if _, ok := uniqueStringsMap[s]; !ok {
				uniqueStringsMap[s] = len(uniqueStrings)
				uniqueStrings = append(uniqueStrings, s)
			}
			factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[s], isObj: true})
		}
	}

	ids, err := m.dict.GetIDs(uniqueStrings)
	if err != nil {
		return fmt.Errorf("failed to encode strings: %w", err)
	}

	batch := m.db.NewWriteBatch()
	defer batch.Cancel()

	for i, fact := range facts {
		sID := ids[factStringRefs[i][0].index]
		pID := ids[factStringRefs[i][1].index]

		var oID uint64
		var isInline bool
		if len(factStringRefs[i]) > 2 && factStringRefs[i][2].isObj {
			oID = ids[factStringRefs[i][2].index]
		} else {
			_, oID, err = m.encodeObject(fact.Object)
			if err != nil {
				return fmt.Errorf("failed to encode object for fact %d: %w", i, err)
			}
			isInline = keys.IsInline(oID)
		}

		// Symmetric TopicID packing: both sID and oID use the same structure.
		// This ensures SPO and OPS indices both achieve data locality per topic.
		// Inline IDs skip packing — the inline flag is in bit 39, not in the topic bits.
		sID = keys.PackID(m.topicID, keys.UnpackLocalID(sID))
		if !isInline {
			oID = keys.PackID(m.topicID, keys.UnpackLocalID(oID))
		}

		// Encode semantic hints for the subject entity
		hints := keys.EncodeSemanticHints(m.defaultEntityType, uint16(keys.HashSemanticName(fact.Subject)), m.defaultFlags)
		value := encodeTripleValueWithHints(0, 0, hints)

		spoKey := keys.EncodeTripleKey(keys.TripleSPOPrefix, sID, pID, oID)
		if err := batch.Set(spoKey, value); err != nil {
			return fmt.Errorf("failed to set SPO key for fact %d: %w", i, err)
		}

		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, sID, pID, oID)
		if err := batch.Set(opsKey, value); err != nil {
			return fmt.Errorf("failed to set OPS key for fact %d: %w", i, err)
		}

		m.numFacts.Add(1)
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	m.persistStatsIfNeeded(uint64(len(facts)))

	if m.config.EnableAutoGC {
		m.factsSinceGC.Add(uint64(len(facts)))
		m.triggerAutoGC()
	}

	return nil
}

func (m *MEBStore) DeleteFactsBySubject(subject string) error {
	slog.Info("deleting facts by subject", "subject", subject)

	sID, err := m.dict.GetID(subject)
	if err != nil {
		slog.Debug("subject not found, nothing to delete", "subject", subject)
		return nil
	}

	// Pack with current topic for symmetric lookup
	sID = keys.PackID(m.topicID, keys.UnpackLocalID(sID))

	txn := m.db.NewTransaction(false)
	defer txn.Discard()

	// Use SPO prefix with full subject ID (all 3 args)
	prefix := keys.EncodeTripleSPOPrefix(sID, 0, 0)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	type tripleKeys struct {
		spo []byte
		ops []byte
	}

	const maxDeleteBatchSize = 1000
	var keysToDelete []tripleKeys
	totalDeleted := 0

	flushBatch := func() error {
		if len(keysToDelete) == 0 {
			return nil
		}

		batchSize := len(keysToDelete)
		deleteTxn := m.db.NewTransaction(true)
		for _, k := range keysToDelete {
			if err := deleteTxn.Delete(k.spo); err != nil {
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete SPO key: %w", err)
			}
			if err := deleteTxn.Delete(k.ops); err != nil {
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete OPS key: %w", err)
			}
		}

		if err := deleteTxn.Commit(); err != nil {
			return fmt.Errorf("failed to commit delete batch: %w", err)
		}

		m.numFacts.Add(^uint64(batchSize - 1))
		totalDeleted += batchSize
		keysToDelete = keysToDelete[:0]
		return nil
	}

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		s, p, o := keys.DecodeTripleKey(key)

		spoKey := make([]byte, len(key))
		copy(spoKey, key)

		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, s, p, o)

		keysToDelete = append(keysToDelete, tripleKeys{
			spo: spoKey,
			ops: opsKey,
		})

		if len(keysToDelete) >= maxDeleteBatchSize {
			if err := flushBatch(); err != nil {
				it.Close()
				return err
			}
		}
	}

	it.Close()

	if err := flushBatch(); err != nil {
		return err
	}

	m.persistStatsIfNeeded(0)

	slog.Info("facts deleted successfully", "subject", subject, "factsDeleted", totalDeleted)
	return nil
}
