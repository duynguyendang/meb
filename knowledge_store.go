package meb

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

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
	if flags&keys.FlagIsDeprecated != 0 {
		return true
	}
	return false
}

// encodeTripleValueWithHints encodes a triple value with semantic hints packed into
// the upper 16 bits of contentOffset. Lower 48 bits hold the actual offset.
func encodeTripleValueWithHints(vectorID uint64, contentOffset uint64, hints uint16) []byte {
	packed := (uint64(hints) << 48) | (contentOffset & ((1 << 48) - 1))
	val := make([]byte, keys.TripleValueSize)
	binary.BigEndian.PutUint64(val[0:8], vectorID)
	binary.BigEndian.PutUint64(val[8:16], packed)
	return val
}

func (m *MEBStore) AddFact(fact Fact) error {
	if err := validateFact(fact); err != nil {
		return fmt.Errorf("failed to add fact: %w", err)
	}
	return m.AddFactBatch([]Fact{fact})
}

func (m *MEBStore) AddFactBatch(facts []Fact) error {
	if m.config.ReadOnly {
		return ErrStoreReadOnly
	}
	if err := validateFacts(facts); err != nil {
		return fmt.Errorf("batch validation failed: %w", err)
	}

	// Write to WAL before any DB operations (dual-DB atomicity)
	for _, fact := range facts {
		objStr, isString := fact.Object.(string)
		if !isString {
			objStr = fmt.Sprintf("%v", fact.Object)
		}
		if err := m.wal.Append(walEntry{
			subject: fact.Subject,
			pred:    fact.Predicate,
			object:  objStr,
		}); err != nil {
			return fmt.Errorf("WAL append failed: %w", err)
		}
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

		sID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(sID))
		if !isInline {
			oID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(oID))
		}

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

	// Clear WAL after successful graph DB write
	if err := m.wal.Clear(); err != nil {
		m.telemetry.Emit("wal_clear_failed", map[string]any{
			"error": err.Error(),
		})
	}

	m.persistStatsIfNeeded(uint64(len(facts)))

	if m.config.EnableAutoGC {
		m.factsSinceGC.Add(uint64(len(facts)))
		m.triggerAutoGC()
	}

	return nil
}

func (m *MEBStore) DeleteFactsBySubject(subject string) error {
	if m.config.ReadOnly {
		return ErrStoreReadOnly
	}
	slog.Info("deleting facts by subject", "subject", subject)

	sID, err := m.dict.GetID(subject)
	if err != nil {
		slog.Debug("subject not found, nothing to delete", "subject", subject)
		return nil
	}

	// Pack with current topic for symmetric lookup
	sID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(sID))

	txn := m.db.NewTransaction(false)
	defer txn.Discard()

	// Use SPO prefix with full subject ID (all 3 args)
	prefix := keys.EncodeTripleSPOPrefix(sID, 0, 0)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	const maxDeleteBatchSize = 1000
	var spoKeys [][]byte
	var opsKeys [][]byte
	pIDs := make([]uint64, 0, maxDeleteBatchSize)
	oIDs := make([]uint64, 0, maxDeleteBatchSize)
	totalDeleted := 0
	referencedIDs := make(map[uint64]int)

	flushBatch := func() error {
		if len(spoKeys) == 0 {
			return nil
		}

		batchSize := len(spoKeys)
		deleteTxn := m.db.NewTransaction(true)
		for i := 0; i < batchSize; i++ {
			if err := deleteTxn.Delete(spoKeys[i]); err != nil {
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete SPO key: %w", err)
			}
			if err := deleteTxn.Delete(opsKeys[i]); err != nil {
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete OPS key: %w", err)
			}
			if pIDs[i] != 0 {
				referencedIDs[pIDs[i]]++
			}
			if oIDs[i] != 0 && !keys.IsInline(oIDs[i]) {
				referencedIDs[oIDs[i]]++
			}
		}

		if err := deleteTxn.Commit(); err != nil {
			return fmt.Errorf("failed to commit delete batch: %w", err)
		}

		m.numFacts.Add(^uint64(batchSize - 1))
		totalDeleted += batchSize
		spoKeys = spoKeys[:0]
		opsKeys = opsKeys[:0]
		pIDs = pIDs[:0]
		oIDs = oIDs[:0]
		return nil
	}

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		s, p, o := keys.DecodeTripleKey(key)

		spoKey := make([]byte, len(key))
		copy(spoKey, key)

		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, s, p, o)

		spoKeys = append(spoKeys, spoKey)
		opsKeys = append(opsKeys, opsKey)
		pIDs = append(pIDs, p)
		oIDs = append(oIDs, o)

		if len(spoKeys) >= maxDeleteBatchSize {
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

	// Clean up orphaned dictionary entries for predicates and objects
	// that are no longer referenced by any other triples
	cleaned := m.cleanupOrphanedDictEntries(referencedIDs)

	slog.Info("facts deleted successfully", "subject", subject, "factsDeleted", totalDeleted, "dictEntriesCleaned", cleaned)
	return nil
}

func (m *MEBStore) cleanupOrphanedDictEntries(referencedIDs map[uint64]int) int {
	if len(referencedIDs) == 0 {
		// Standalone scan: find all dictionary entries and check if they're referenced
		return m.cleanupOrphanedDictEntriesFullScan()
	}

	// Convert packed IDs to local IDs and deduplicate.
	// The dictionary stores local IDs, so we check if each local ID
	// is still referenced by any remaining triple in the graph.
	localCandidates := make([]uint64, 0, len(referencedIDs))
	seen := make(map[uint64]bool)
	for packedID := range referencedIDs {
		localID := keys.UnpackLocalID(packedID)
		if localID != 0 && !seen[localID] {
			seen[localID] = true
			localCandidates = append(localCandidates, localID)
		}
	}

	if len(localCandidates) == 0 {
		return 0
	}

	// Batch all reference checks into a single read transaction.
	// For each candidate, check both SPO (as predicate) and OPS (as object) indices.
	unreferenced := make([]uint64, 0, len(localCandidates))
	m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for _, localID := range localCandidates {
			stillReferenced := false

			// Check SPO index: is this ID used as a predicate?
			prefix := keys.EncodeTripleSPOPrefix(0, localID, 0)
			it.Seek(prefix)
			if it.ValidForPrefix(prefix) {
				stillReferenced = true
			}

			if !stillReferenced {
				// Check OPS index: is this ID used as an object?
				opsPrefix := keys.EncodeTripleOPSPrefix(localID, 0, 0)
				it.Seek(opsPrefix)
				if it.ValidForPrefix(opsPrefix) {
					stillReferenced = true
				}
			}

			if !stillReferenced {
				unreferenced = append(unreferenced, localID)
			}
		}
		return nil
	})

	cleaned := 0
	for _, localID := range unreferenced {
		s, err := m.dict.GetString(localID)
		if err != nil {
			continue
		}
		if err := m.dict.DeleteID(s); err == nil {
			cleaned++
		}
	}

	return cleaned
}

// cleanupOrphanedDictEntriesFullScan scans all dictionary entries and removes
// those that are not referenced by any triple in the SPO/OPS indices.
func (m *MEBStore) cleanupOrphanedDictEntriesFullScan() int {
	// Collect all local IDs from the dictionary (reverse index: ID → string)
	var candidateIDs []uint64
	m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		revPrefix := []byte{keys.ReverseDictPrefix} // 0x81
		for it.Seek(revPrefix); it.ValidForPrefix(revPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) < 9 { // 1 prefix + 8 ID bytes
				continue
			}
			id := binary.BigEndian.Uint64(key[1:9])
			candidateIDs = append(candidateIDs, id)
		}
		return nil
	})

	if len(candidateIDs) == 0 {
		return 0
	}

	// Check which IDs are still referenced by triples
	unreferenced := make([]uint64, 0, len(candidateIDs))
	m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for _, localID := range candidateIDs {
			stillReferenced := false

			// Check SPO index: is this ID used as a predicate?
			prefix := keys.EncodeTripleSPOPrefix(0, localID, 0)
			it.Seek(prefix)
			if it.ValidForPrefix(prefix) {
				stillReferenced = true
			}

			if !stillReferenced {
				// Check OPS index: is this ID used as an object?
				opsPrefix := keys.EncodeTripleOPSPrefix(localID, 0, 0)
				it.Seek(opsPrefix)
				if it.ValidForPrefix(opsPrefix) {
					stillReferenced = true
				}
			}

			// Also check as a subject (SPO with subject = localID)
			if !stillReferenced {
				spoPrefix := keys.EncodeTripleSPOPrefix(localID, 0, 0)
				it.Seek(spoPrefix)
				if it.ValidForPrefix(spoPrefix) {
					stillReferenced = true
				}
			}

			if !stillReferenced {
				unreferenced = append(unreferenced, localID)
			}
		}
		return nil
	})

	cleaned := 0
	for _, localID := range unreferenced {
		s, err := m.dict.GetString(localID)
		if err != nil {
			continue
		}
		if err := m.dict.DeleteID(s); err == nil {
			cleaned++
		}
	}

	return cleaned
}

// cleanupOrphanedVectors scans vectors and removes those that have no
// corresponding content or any referencing facts.
func (m *MEBStore) cleanupOrphanedVectors() int {
	cleaned := 0

	m.withWriteTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		vecPrefix := []byte{keys.VectorFullPrefix}
		var orphanKeys [][]byte

		for it.Seek(vecPrefix); it.ValidForPrefix(vecPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) != keys.ChunkKeySize {
				continue
			}

			id := binary.BigEndian.Uint64(key[1:])

			// Check if content exists for this vector
			contentKey := keys.EncodeChunkKey(id)
			_, err := txn.Get(contentKey)
			hasContent := err == nil

			// Check if any fact references this ID (as object, subject is string so check as subject)
			hasFact := false
			if !hasContent {
				// Check SPO: is this ID used as a subject (packed with any topic)?
				// Since subjects are packed with topicID, we need to scan all topics
				spoPrefix := []byte{keys.TripleSPOPrefix}
				subIt := txn.NewIterator(badger.DefaultIteratorOptions)
				for subIt.Seek(spoPrefix); subIt.ValidForPrefix(spoPrefix); subIt.Next() {
					sKey := subIt.Item().Key()
					if len(sKey) != keys.TripleKeySize {
						continue
					}
					s, _, _ := keys.DecodeTripleKey(sKey)
					if keys.UnpackLocalID(s) == id {
						hasFact = true
						break
					}
				}
				subIt.Close()
			}

			if !hasContent && !hasFact {
				orphanKeys = append(orphanKeys, key)
			}
		}

		// Delete orphans
		for _, key := range orphanKeys {
			if err := txn.Delete(key); err != nil {
				return err
			}
			cleaned++
		}

		return nil
	})

	return cleaned
}
