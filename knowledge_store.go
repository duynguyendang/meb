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
	entries := make([]walEntry, len(facts))
	for i, fact := range facts {
		objStr := fmt.Sprintf("%v", fact.Object)
		var objType uint8
		switch fact.Object.(type) {
		case string:
			objType = walObjString
		case bool:
			objType = walObjBool
		case int32:
			objType = walObjInt32
		case float32:
			objType = walObjFloat32
		}
		entries[i] = walEntry{
			subject: fact.Subject,
			pred:    fact.Predicate,
			object:  objStr,
			objType: objType,
		}
	}
	if err := m.wal.AppendBatch(entries); err != nil {
		return fmt.Errorf("WAL batch append failed: %w", err)
	}

	if err := m.addFactBatchInternal(facts); err != nil {
		return err
	}

	// Clear WAL after successful graph DB write
	if err := m.wal.Clear(); err != nil {
		m.telemetry.Emit("wal_clear_failed", map[string]any{
			"error": err.Error(),
		})
	}

	return nil
}

// addFactBatchInternal writes facts to Badger without touching the WAL.
// Used by both AddFactBatch (after WAL write) and WAL replay (which supplies
// its own commit guarantees).
func (m *MEBStore) addFactBatchInternal(facts []Fact) error {
	factRefs, uniqueStrings := collectStringRefs(facts)

	ids, err := m.dict.GetIDs(uniqueStrings)
	if err != nil {
		return fmt.Errorf("failed to encode strings: %w", err)
	}

	fks, err := m.encodeFactKeys(facts, factRefs, ids)
	if err != nil {
		return err
	}

	batch := m.db.NewWriteBatch()
	defer batch.Cancel()

	for i, fk := range fks {
		if err := batch.Set(fk.spo, fk.value); err != nil {
			return fmt.Errorf("failed to set SPO key for fact %d: %w", i, err)
		}
		if err := batch.Set(fk.ops, fk.value); err != nil {
			return fmt.Errorf("failed to set OPS key for fact %d: %w", i, err)
		}
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	// Count AFTER successful flush to avoid overcounting on failure.
	m.numFacts.Add(uint64(len(facts)))

	m.persistStatsIfNeeded(uint64(len(facts)))

	if m.config.EnableAutoGC {
		m.factsSinceGC.Add(uint64(len(facts)))
		m.triggerAutoGC()
	}

	return nil
}

// DeleteFactWithTopic deletes a single exact fact from the specified topic and
// returns true if the triple existed and was deleted.
func (m *MEBStore) DeleteFactWithTopic(fact Fact, topicID uint32) (bool, error) {
	if m.config.ReadOnly {
		return false, ErrStoreReadOnly
	}
	if err := validateFact(fact); err != nil {
		return false, err
	}
	if topicID == 0 {
		topicID = m.topicID.Load()
	}

	sID, err := m.dict.GetID(fact.Subject)
	if err != nil {
		return false, nil
	}
	pID, err := m.dict.GetID(fact.Predicate)
	if err != nil {
		return false, nil
	}

	var oID uint64
	if sObj, ok := fact.Object.(string); ok {
		id, err := m.dict.GetID(sObj)
		if err != nil {
			return false, nil
		}
		oID = keys.PackID(topicID, keys.UnpackLocalID(id))
	} else {
		objStr, encodedID, err := m.encodeObject(fact.Object)
		if err != nil {
			return false, err
		}
		if encodedID != 0 {
			oID = encodedID
		} else {
			id, err := m.dict.GetID(objStr)
			if err != nil {
				return false, nil
			}
			oID = keys.PackID(topicID, keys.UnpackLocalID(id))
		}
	}

	sID = keys.PackID(topicID, keys.UnpackLocalID(sID))
	spoKey := keys.EncodeTripleKey(keys.TripleSPOPrefix, sID, pID, oID)
	opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, sID, pID, oID)

	deleted := false
	err = m.withWriteTxn(func(txn *badger.Txn) error {
		if _, err := txn.Get(spoKey); err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		if err := txn.Delete(spoKey); err != nil {
			return err
		}
		if err := txn.Delete(opsKey); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if deleted {
		m.numFacts.Add(^uint64(0))
		m.persistStatsIfNeeded(0)
	}
	return deleted, nil
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
	cleaned, err := m.cleanupOrphanedDictEntries(referencedIDs)
	if err != nil {
		slog.Warn("orphaned dict cleanup skipped", "error", err)
	}

	slog.Info("facts deleted successfully", "subject", subject, "factsDeleted", totalDeleted, "dictEntriesCleaned", cleaned)
	return nil
}

func (m *MEBStore) cleanupOrphanedDictEntries(referencedIDs map[uint64]int) (int, error) {
	if len(referencedIDs) == 0 {
		return m.cleanupOrphanedDictEntriesFullScan()
	}

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
		return 0, nil
	}

	candidateSet := make(map[uint64]bool, len(localCandidates))
	for _, id := range localCandidates {
		candidateSet[id] = true
	}

	// Single scan over SPO+OPS to build set of all referenced local IDs,
	// then check candidates against that set. O(N + M) instead of O(N*M).
	referencedSet := make(map[uint64]bool)
	if err := m.buildReferencedLocalIDSet(referencedSet); err != nil {
		return 0, fmt.Errorf("build referenced set: %w", err)
	}

	unreferenced := make([]uint64, 0, len(localCandidates))
	for _, localID := range localCandidates {
		if !referencedSet[localID] {
			unreferenced = append(unreferenced, localID)
		}
	}

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

	return cleaned, nil
}

// buildReferencedLocalIDSet scans all triple keys and collects every referenced local ID
// (subjects, predicates, and objects) into the given set.
func (m *MEBStore) buildReferencedLocalIDSet(refs map[uint64]bool) error {
	return m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for _, prefix := range [][]byte{{keys.TripleSPOPrefix}, {keys.TripleOPSPrefix}} {
			it.Seek(prefix)
			for ; it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				if len(key) != keys.TripleKeySize {
					continue
				}
				s, p, o := keys.DecodeTripleKey(key)
				refs[keys.UnpackLocalID(s)] = true
				refs[keys.UnpackLocalID(p)] = true
				if !keys.IsInline(o) {
					refs[keys.UnpackLocalID(o)] = true
				}
			}
		}
		return nil
	})
}

// cleanupOrphanedDictEntriesFullScan scans all dictionary entries and removes
// those not referenced by any triple, content, or vector key.
// Throttled: max 1000 orphans per cycle.
func (m *MEBStore) cleanupOrphanedDictEntriesFullScan() (int, error) {
	const maxOrphansPerCycle = 1000

	var candidateIDs []uint64
	if err := m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		revPrefix := []byte{keys.ReverseDictPrefix}
		for it.Seek(revPrefix); it.ValidForPrefix(revPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) < 9 {
				continue
			}
			id := binary.BigEndian.Uint64(key[1:9])
			candidateIDs = append(candidateIDs, id)
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan dict candidates: %w", err)
	}

	if len(candidateIDs) == 0 {
		return 0, nil
	}

	// Build set of all referenced local IDs (triples + content + vectors)
	referencedSet := make(map[uint64]bool)
	if err := m.buildReferencedLocalIDSet(referencedSet); err != nil {
		return 0, fmt.Errorf("build referenced set: %w", err)
	}

	// Also check content (0x10) and vector (0x11) keys
	if err := m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for _, pfx := range [][]byte{{keys.ChunkPrefix}, {keys.VectorFullPrefix}} {
			it.Seek(pfx)
			for ; it.ValidForPrefix(pfx); it.Next() {
				key := it.Item().Key()
				if len(key) != keys.ChunkKeySize {
					continue
				}
				id := binary.BigEndian.Uint64(key[1:])
				referencedSet[id] = true
			}
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan content/vector refs: %w", err)
	}

	unreferenced := make([]uint64, 0, maxOrphansPerCycle)
	for _, localID := range candidateIDs {
		if !referencedSet[localID] && len(unreferenced) < maxOrphansPerCycle {
			unreferenced = append(unreferenced, localID)
		}
	}

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

	return cleaned, nil
}

// cleanupOrphanedVectors scans vectors (0x11) and removes those that have no
// corresponding content (0x10) or any referencing fact subject in SPO (0x20).
//
// Uses an O(1)-memory streaming merge-join instead of materializing all subject IDs
// in a heap map: both 0x11 and per-topic 0x20 keys are stored sorted by 40-bit
// LocalID in BadgerDB, so concurrent iterators advance in lock-step with zero
// per-node RAM allocation.
func (m *MEBStore) cleanupOrphanedVectors() int {
	cleaned := 0
	const maxOrphansPerCycle = 1000
	const maxStreamingTopicCursors = 16

	var orphanIDs []uint64

	_ = m.withWriteTxn(func(txn *badger.Txn) error {
		// 1. Discover active topicIDs in the SPO (0x20) index in O(numTopics) seeks.
		topics := discoverSPOTopics(txn)

		var cursors []*spoTopicCursor
		var overflowTopics []uint32
		defer func() {
			for _, tc := range cursors {
				tc.it.Close()
			}
		}()

		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false

		for i, tid := range topics {
			if i >= maxStreamingTopicCursors {
				overflowTopics = topics[i:]
				break
			}
			pfx := spoTopicPrefix3(tid)
			it := txn.NewIterator(itOpts)
			it.Seek(pfx)
			tc := &spoTopicCursor{
				topicID: tid,
				prefix:  pfx,
				it:      it,
			}
			tc.advanceValid()
			cursors = append(cursors, tc)
		}

		// Reset/rewind cursor for exact topic when vecTopic changes in 0x11.
		findCursor := func(tid uint32) *spoTopicCursor {
			for _, tc := range cursors {
				if tc.topicID == tid {
					return tc
				}
			}
			return nil
		}

		cursorHasLocalID := func(tc *spoTopicCursor, targetLocal uint64) bool {
			if !tc.valid || tc.curID > targetLocal {
				return false
			}
			if tc.curID == targetLocal {
				return true
			}
			// Jump directly via Seek if far behind, otherwise step sequentially.
			if targetLocal-tc.curID > 16 {
				seekKey := keys.EncodeTripleSPOPrefix(keys.PackID(tc.topicID, targetLocal), 0, 0)
				tc.it.Seek(seekKey)
				tc.advanceValid()
			} else {
				for tc.valid && tc.curID < targetLocal {
					tc.it.Next()
					tc.advanceValid()
				}
			}
			return tc.valid && tc.curID == targetLocal
		}

		// Rewind cursors whenever vecTopic switches in 0x11 so packed topics can merge sequentially too.
		var lastVecTopic uint32
		firstVec := true

		vecIt := txn.NewIterator(itOpts)
		defer vecIt.Close()

		vecPrefix := []byte{keys.VectorFullPrefix}
		var orphanKeys [][]byte

		for vecIt.Seek(vecPrefix); vecIt.ValidForPrefix(vecPrefix); vecIt.Next() {
			rawKey := vecIt.Item().Key()
			if len(rawKey) != keys.ChunkKeySize {
				continue
			}

			vecID := binary.BigEndian.Uint64(rawKey[1:])
			vecTopic := keys.UnpackTopicID(vecID)
			vecLocal := keys.UnpackLocalID(vecID)

			if firstVec || vecTopic != lastVecTopic {
				firstVec = false
				lastVecTopic = vecTopic
				if vecTopic != 0 {
					if tc := findCursor(vecTopic); tc != nil && tc.curID > vecLocal {
						tc.it.Seek(tc.prefix)
						tc.advanceValid()
					}
				}
			}

			// Check 0x10 content existence first (O(1) point lookup).
			if _, err := txn.Get(keys.EncodeChunkKey(vecID)); err == nil {
				continue
			}
			if vecTopic != 0 {
				if _, err := txn.Get(keys.EncodeChunkKey(vecLocal)); err == nil {
					continue
				}
			}

			// Streaming merge-join check against SPO (0x20) sorted LocalIDs.
			hasFact := false
			if vecTopic == 0 {
				// Unpacked document/vector ID: check across active topics in lock-step.
				for _, tc := range cursors {
					if cursorHasLocalID(tc, vecLocal) {
						hasFact = true
						break
					}
				}
				if !hasFact {
					for _, tid := range overflowTopics {
						if spoTopicHasSubject(txn, tid, vecLocal) {
							hasFact = true
							break
						}
					}
				}
			} else {
				if tc := findCursor(vecTopic); tc != nil {
					hasFact = cursorHasLocalID(tc, vecLocal)
				} else {
					hasFact = spoTopicHasSubject(txn, vecTopic, vecLocal)
				}
			}

			if !hasFact {
				kCopy := make([]byte, len(rawKey))
				copy(kCopy, rawKey)
				orphanKeys = append(orphanKeys, kCopy)
				orphanIDs = append(orphanIDs, vecID)
				if len(orphanKeys) >= maxOrphansPerCycle {
					break
				}
			}
		}

		for _, key := range orphanKeys {
			if err := txn.Delete(key); err != nil {
				return err
			}
			cleaned++
		}

		return nil
	})

	if cleaned > 0 && m.vectors != nil {
		for _, id := range orphanIDs {
			m.vectors.Delete(id)
		}
	}

	return cleaned
}

func spoTopicPrefix3(topicID uint32) []byte {
	return []byte{
		keys.TripleSPOPrefix,
		byte(topicID >> 16),
		byte(topicID >> 8),
		byte(topicID),
	}
}

func discoverSPOTopics(txn *badger.Txn) []uint32 {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var topics []uint32
	seekKey := []byte{keys.TripleSPOPrefix}
	for {
		it.Seek(seekKey)
		if !it.ValidForPrefix([]byte{keys.TripleSPOPrefix}) {
			break
		}
		key := it.Item().Key()
		if len(key) != keys.TripleKeySize {
			it.Next()
			if !it.Valid() {
				break
			}
			seekKey = it.Item().KeyCopy(nil)
			continue
		}
		s, _, _ := keys.DecodeTripleKey(key)
		tid := keys.UnpackTopicID(s)
		topics = append(topics, tid)
		if tid == 0xFFFFFF {
			break
		}
		seekKey = spoTopicPrefix3(tid + 1)
	}
	return topics
}

type spoTopicCursor struct {
	topicID uint32
	prefix  []byte
	it      *badger.Iterator
	valid   bool
	curID   uint64
}

func (tc *spoTopicCursor) advanceValid() {
	for tc.it.ValidForPrefix(tc.prefix) {
		k := tc.it.Item().Key()
		if len(k) == keys.TripleKeySize {
			s, _, _ := keys.DecodeTripleKey(k)
			tc.curID = keys.UnpackLocalID(s)
			tc.valid = true
			return
		}
		tc.it.Next()
	}
	tc.valid = false
}

func spoTopicHasSubject(txn *badger.Txn, topicID uint32, localID uint64) bool {
	sID := keys.PackID(topicID, localID)
	pfx := keys.EncodeTripleSPOPrefix(sID, 0, 0)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.PrefetchSize = 1
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(pfx)
	return it.ValidForPrefix(pfx)
}
