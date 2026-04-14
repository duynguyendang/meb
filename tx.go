package meb

import (
	"fmt"

	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/vector"

	"github.com/dgraph-io/badger/v4"
	"github.com/klauspost/compress/s2"
)

// StoreTxn wraps a BadgerDB transaction with store-specific helpers.
// Created via MEBStore.View() or MEBStore.Update().
type StoreTxn struct {
	txn   *badger.Txn
	store *MEBStore
}

// --- Internal helpers (kept for existing code) ---

func (m *MEBStore) withReadTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(false)
	defer txn.Discard()
	return fn(txn)
}

func (m *MEBStore) withWriteTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(true)
	if err := fn(txn); err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}

// --- Public transaction API ---

// View executes a read-only transaction.
func (m *MEBStore) View(fn func(*StoreTxn) error) error {
	txn := m.db.NewTransaction(false)
	defer txn.Discard()
	return fn(&StoreTxn{txn: txn, store: m})
}

// Update executes a read-write transaction with automatic commit/rollback.
// If fn returns an error, the transaction is discarded (rolled back).
// Otherwise, the transaction is committed.
func (m *MEBStore) Update(fn func(*StoreTxn) error) error {
	txn := m.db.NewTransaction(true)
	storedCount := m.numFacts.Load()
	if err := fn(&StoreTxn{txn: txn, store: m}); err != nil {
		m.numFacts.Store(storedCount) // restore counter on rollback
		txn.Discard()
		return err
	}
	return txn.Commit()
}

// --- StoreTxn methods wrapping existing store operations ---

// Scan iterates over facts matching the given subject, predicate, object.
// Uses the store's current topicID.
func (t *StoreTxn) Scan(s, p, o string) iterSeq2FactError {
	return t.store.scanWithTxn(t.txn, s, p, o, t.store.topicID.Load())
}

// ScanInTopic iterates over facts matching the given subject, predicate, object
// within the specified topic.
func (t *StoreTxn) ScanInTopic(topicID uint32, s, p, o string) iterSeq2FactError {
	return t.store.scanWithTxn(t.txn, s, p, o, topicID)
}

// AddFact adds a single fact within the transaction.
func (t *StoreTxn) AddFact(fact Fact) error {
	return t.AddFactBatch([]Fact{fact})
}

// AddFactBatch adds multiple facts within the transaction.
// Uses the store's current topicID.
func (t *StoreTxn) AddFactBatch(facts []Fact) error {
	return t.store.addFactBatchInTxn(t.txn, facts, t.store.topicID.Load())
}

// AddFactBatchWithTopic adds multiple facts within the transaction using a specific topicID.
func (t *StoreTxn) AddFactBatchWithTopic(facts []Fact, topicID uint32) error {
	return t.store.addFactBatchInTxn(t.txn, facts, topicID)
}

// DeleteFactsBySubject deletes all facts for the given subject within the transaction.
func (t *StoreTxn) DeleteFactsBySubject(subject string) error {
	return t.store.deleteFactsBySubjectInTxn(t.txn, subject)
}

// SetContent stores content by ID within the transaction.
func (t *StoreTxn) SetContent(id uint64, data []byte) error {
	return t.store.setContentInTxn(t.txn, id, data)
}

// GetContent retrieves content by ID within the transaction.
func (t *StoreTxn) GetContent(id uint64) ([]byte, error) {
	return t.store.getContentInTxn(t.txn, id)
}

// AddVector adds a vector within the transaction.
func (t *StoreTxn) AddVector(id uint64, vec []float32) error {
	return t.store.addVectorInTxn(t.txn, id, vec)
}

// AddVectorWithHash adds a vector with a semantic hash within the transaction.
func (t *StoreTxn) AddVectorWithHash(id uint64, vec []float32, semanticHash uint8) error {
	return t.store.addVectorInTxnWithHash(t.txn, id, vec, semanticHash)
}

// DeleteVector deletes a vector within the transaction.
func (t *StoreTxn) DeleteVector(id uint64) bool {
	return t.store.deleteVectorInTxn(t.txn, id)
}

// HasVector checks if a vector exists within the transaction.
func (t *StoreTxn) HasVector(id uint64) bool {
	return t.store.vectors.HasVector(id)
}

// GetOrCreateID gets or creates a dictionary ID for the given string within the transaction.
func (t *StoreTxn) GetOrCreateID(s string) (uint64, error) {
	return t.store.dictGetOrCreateIDInTxn(t.txn, s)
}

// GetID gets a dictionary ID for the given string within the transaction.
func (t *StoreTxn) GetID(s string) (uint64, error) {
	return t.store.dictGetIDInTxn(t.txn, s)
}

// GetString resolves a dictionary ID to its string within the transaction.
func (t *StoreTxn) GetString(id uint64) (string, error) {
	return t.store.dictGetStringInTxn(t.txn, id)
}

// DeleteDocument atomically deletes a document (content, vector, facts, dict entry)
// within the transaction. Uses the store's current topicID.
func (t *StoreTxn) DeleteDocument(docKey string) error {
	return t.store.deleteDocumentInTxn(t.txn, docKey, t.store.topicID.Load())
}

// DeleteDocumentWithTopic atomically deletes a document using a specific topicID.
func (t *StoreTxn) DeleteDocumentWithTopic(docKey string, topicID uint32) error {
	return t.store.deleteDocumentInTxn(t.txn, docKey, topicID)
}

// Exists checks if a fact exists within the transaction.
func (t *StoreTxn) Exists(s, p, o string) bool {
	return t.store.existsInTxn(t.txn, s, p, o)
}

// --- Internal helpers that operate on a given badger.Txn ---

func (m *MEBStore) scanWithTxn(txn *badger.Txn, s, p, o string, topicID uint32) iterSeq2FactError {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}

	// Pack IDs with the specified topic
	if sBound {
		sID = keys.PackID(topicID, keys.UnpackLocalID(sID))
	}
	if oBound {
		oID = keys.PackID(topicID, keys.UnpackLocalID(oID))
	}

	strategy := selectScanStrategy(sBound, pBound, oBound, sID, pID, oID)

	opts := &scanOptions{
		s:        s,
		p:        p,
		o:        o,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		strategy: strategy,
		topicID:  topicID,
	}

	return m.scanImplWithTxn(txn, opts)
}

func (m *MEBStore) scanImplWithTxn(txn *badger.Txn, opts *scanOptions) iterSeq2FactError {
	return func(yield func(Fact, error) bool) {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true
		it := txn.NewIterator(itOpts)
		defer it.Close()

		for it.Seek(opts.strategy.prefix); it.ValidForPrefix(opts.strategy.prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			if len(key) != keys.TripleKeySize {
				continue
			}

			var result scanResult
			result.foundSID, result.foundPID, result.foundOID = keys.DecodeTripleKey(key)

			if opts.sBound && result.foundSID != opts.sID {
				continue
			}
			if opts.pBound && result.foundPID != opts.pID {
				continue
			}
			if opts.oBound && result.foundOID != opts.oID {
				continue
			}

			result.key = key
			fact, err := m.resolveFactStrings(opts, &result)
			if err != nil {
				yield(Fact{}, err)
				return
			}

			if !yield(fact, nil) {
				return
			}
		}
	}
}

type iterSeq2FactError = func(yield func(Fact, error) bool)

func (m *MEBStore) addFactBatchInTxn(txn *badger.Txn, facts []Fact, topicID uint32) error {
	if err := validateFacts(facts); err != nil {
		return fmt.Errorf("batch validation failed: %w", err)
	}

	// Deduplicate strings
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

		sID = keys.PackID(topicID, keys.UnpackLocalID(sID))
		if !isInline {
			oID = keys.PackID(topicID, keys.UnpackLocalID(oID))
		}

		hints := keys.EncodeSemanticHints(m.defaultEntityType, uint16(keys.HashSemanticName(fact.Subject)), m.defaultFlags)
		value := encodeTripleValueWithHints(0, 0, hints)

		spoKey := keys.EncodeTripleKey(keys.TripleSPOPrefix, sID, pID, oID)
		if err := txn.Set(spoKey, value); err != nil {
			return fmt.Errorf("failed to set SPO key for fact %d: %w", i, err)
		}

		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, sID, pID, oID)
		if err := txn.Set(opsKey, value); err != nil {
			return fmt.Errorf("failed to set OPS key for fact %d: %w", i, err)
		}

		m.numFacts.Add(1)
	}

	return nil
}

func (m *MEBStore) deleteFactsBySubjectInTxn(txn *badger.Txn, subject string) error {
	sID, err := m.dict.GetID(subject)
	if err != nil {
		// Subject not in dictionary, nothing to delete
		return nil
	}

	sID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(sID))

	prefix := keys.EncodeTripleSPOPrefix(sID, 0, 0)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	const maxDeleteBatchSize = 1000
	var spoKeys [][]byte
	var opsKeys [][]byte
	totalDeleted := 0

	flushBatch := func() error {
		if len(spoKeys) == 0 {
			return nil
		}

		for i := 0; i < len(spoKeys); i++ {
			if err := txn.Delete(spoKeys[i]); err != nil {
				return fmt.Errorf("failed to delete SPO key: %w", err)
			}
			if err := txn.Delete(opsKeys[i]); err != nil {
				return fmt.Errorf("failed to delete OPS key: %w", err)
			}
		}

		m.numFacts.Add(^uint64(len(spoKeys) - 1))
		totalDeleted += len(spoKeys)
		spoKeys = spoKeys[:0]
		opsKeys = opsKeys[:0]
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

	_ = totalDeleted
	return nil
}

func (m *MEBStore) setContentInTxn(txn *badger.Txn, id uint64, data []byte) error {
	compressed := s2.Encode(nil, data)
	key := keys.EncodeChunkKey(id)
	return txn.Set(key, compressed)
}

func (m *MEBStore) getContentInTxn(txn *badger.Txn, id uint64) ([]byte, error) {
	key := keys.EncodeChunkKey(id)
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return s2.Decode(nil, data)
}

func (m *MEBStore) addVectorInTxn(txn *badger.Txn, id uint64, vec []float32) error {
	return m.addVectorInTxnWithHash(txn, id, vec, 0)
}

func (m *MEBStore) addVectorInTxnWithHash(txn *badger.Txn, id uint64, vec []float32, semanticHash uint8) error {
	if len(vec) != m.vectors.FullDim() {
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", m.vectors.FullDim(), len(vec))
	}

	hybridCfg := m.vectors.HybridConfig()
	hybridData := vector.QuantizeHybrid(vec, hybridCfg)
	hybridSize := len(hybridData)

	valueBuf := make([]byte, 1+hybridSize)
	valueBuf[0] = semanticHash
	copy(valueBuf[1:], hybridData)

	vecKey := keys.EncodeVectorFullKey(id)
	return txn.Set(vecKey, valueBuf)
}

func (m *MEBStore) deleteVectorInTxn(txn *badger.Txn, id uint64) bool {
	if !m.vectors.HasVector(id) {
		return false
	}

	// Delete vector from BadgerDB
	vecKey := keys.EncodeVectorFullKey(id)
	if err := txn.Delete(vecKey); err != nil {
		return false
	}

	// Delete from in-memory registry
	return m.vectors.Delete(id)
}

func (m *MEBStore) dictGetOrCreateIDInTxn(txn *badger.Txn, s string) (uint64, error) {
	return m.dict.(*dict.Encoder).GetOrCreateIDInTxn(txn, s)
}

func (m *MEBStore) dictGetIDInTxn(txn *badger.Txn, s string) (uint64, error) {
	return m.dict.(*dict.Encoder).GetIDInTxn(txn, s)
}

func (m *MEBStore) dictGetStringInTxn(txn *badger.Txn, id uint64) (string, error) {
	return m.dict.(*dict.Encoder).GetStringInTxn(txn, id)
}

func (m *MEBStore) existsInTxn(txn *badger.Txn, s, p, o string) bool {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return false
	}

	if sBound {
		sID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(sID))
	}
	if oBound {
		oID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(oID))
	}

	prefix := keys.EncodeTripleSPOPrefix(sID, pID, oID)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.PrefetchSize = 1
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		if len(key) != keys.TripleKeySize {
			continue
		}
		fs, fp, fo := keys.DecodeTripleKey(key)
		if sBound && fs != sID {
			continue
		}
		if pBound && fp != pID {
			continue
		}
		if oBound && fo != oID {
			continue
		}
		return true
	}

	return false
}

func (m *MEBStore) deleteDocumentInTxn(txn *badger.Txn, docKey string, topicID uint32) error {
	id, err := m.dict.GetID(docKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	// Delete content
	contentKey := keys.EncodeChunkKey(id)
	_ = txn.Delete(contentKey)

	// Delete vector from BadgerDB
	vecKey := keys.EncodeVectorFullKey(id)
	_ = txn.Delete(vecKey)

	// Delete vector from in-memory registry (safe guard: check first)
	if m.vectors.HasVector(id) {
		m.vectors.Delete(id)
	}

	// Delete metadata facts
	packedID := keys.PackID(topicID, keys.UnpackLocalID(id))
	metadataPrefix := keys.EncodeTripleSPOPrefix(packedID, 0, 0)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var deletedCount uint64
	for it.Seek(metadataPrefix); it.ValidForPrefix(metadataPrefix); it.Next() {
		item := it.Item()
		key := item.Key()
		s, p, o := keys.DecodeTripleKey(key)
		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, s, p, o)
		if err := txn.Delete(key); err != nil {
			return err
		}
		if err := txn.Delete(opsKey); err != nil {
			return err
		}
		deletedCount++
	}

	if deletedCount > 0 {
		m.numFacts.Add(^uint64(deletedCount - 1))
	}

	// Delete dictionary entries
	_ = m.dict.(*dict.Encoder).DeleteIDInTxn(txn, docKey)

	return nil
}
