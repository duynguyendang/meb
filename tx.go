package meb

import (
	"context"
	"fmt"
	"log/slog"

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

	// factDelta tracks net additions (positive) or deletions (negative) made within
	// this transaction. Applied only on commit — never touches m.numFacts mid-txn.
	factDelta int64

	// Post-commit side effects: applied only after txn.Commit() succeeds.
	postCommitFns []func()
}

// addPostCommit registers a function to run after the transaction commits successfully.
func (t *StoreTxn) addPostCommit(fn func()) {
	t.postCommitFns = append(t.postCommitFns, fn)
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
// In-memory side effects (counters, mmap caches, dict caches) are applied only
// after the Badger transaction commits successfully, using accumulated deltas.
func (m *MEBStore) Update(fn func(*StoreTxn) error) error {
	txn := m.db.NewTransaction(true)
	st := &StoreTxn{txn: txn, store: m}
	if err := fn(st); err != nil {
		txn.Discard()
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	// Apply fact count delta
	if st.factDelta > 0 {
		m.numFacts.Add(uint64(st.factDelta))
	} else if st.factDelta < 0 {
		m.numFacts.Add(^uint64(-st.factDelta - 1))
	}
	// Apply post-commit side effects (mmap cache updates, dict cache finalization, etc.)
	for _, fn := range st.postCommitFns {
		fn()
	}
	return nil
}

// --- StoreTxn methods wrapping existing store operations ---

// Scan iterates over facts matching the given subject, predicate, object.
// Uses the store's current topicID.
func (t *StoreTxn) Scan(ctx context.Context, s, p, o string) iterSeq2FactError {
	return t.store.scanWithTxn(ctx, t.txn, s, p, o, t.store.topicID.Load())
}

// ScanInTopic iterates over facts matching the given subject, predicate, object
// within the specified topic.
func (t *StoreTxn) ScanInTopic(ctx context.Context, topicID uint32, s, p, o string) iterSeq2FactError {
	return t.store.scanWithTxn(ctx, t.txn, s, p, o, topicID)
}

// AddFact adds a single fact within the transaction.
func (t *StoreTxn) AddFact(fact Fact) error {
	return t.AddFactBatch([]Fact{fact})
}

// AddFactBatch adds multiple facts within the transaction.
// Uses the store's current topicID.
func (t *StoreTxn) AddFactBatch(facts []Fact) error {
	added, err := t.store.addFactBatchInTxnCounted(t.txn, facts, t.store.topicID.Load())
	t.factDelta += int64(added)
	return err
}

// AddFactBatchWithTopic adds multiple facts within the transaction using a specific topicID.
func (t *StoreTxn) AddFactBatchWithTopic(facts []Fact, topicID uint32) error {
	added, err := t.store.addFactBatchInTxnCounted(t.txn, facts, topicID)
	t.factDelta += int64(added)
	return err
}

// DeleteFactsBySubject deletes all facts for the given subject within the transaction.
func (t *StoreTxn) DeleteFactsBySubject(subject string) error {
	deleted, err := t.store.deleteFactsBySubjectInTxn(t.txn, subject)
	t.factDelta -= int64(deleted)
	return err
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
// The Badger write is done in-txn; the mmap cache update is deferred to post-commit.
func (t *StoreTxn) AddVector(id uint64, vec []float32) error {
	if err := t.store.addVectorInTxn(t.txn, id, vec); err != nil {
		return err
	}
	st := t.store
	v := make([]float32, len(vec))
	copy(v, vec)
	t.addPostCommit(func() {
		if err := st.vectors.AddToMmapCache(id, v, 0); err != nil {
			slog.Warn("post-commit mmap cache update failed", "id", id, "error", err)
		}
	})
	return nil
}

// AddVectorWithHash adds a vector with a semantic hash within the transaction.
func (t *StoreTxn) AddVectorWithHash(id uint64, vec []float32, semanticHash uint8) error {
	if err := t.store.addVectorInTxnWithHash(t.txn, id, vec, semanticHash); err != nil {
		return err
	}
	st := t.store
	v := make([]float32, len(vec))
	copy(v, vec)
	t.addPostCommit(func() {
		if err := st.vectors.AddToMmapCache(id, v, semanticHash); err != nil {
			slog.Warn("post-commit mmap cache update failed", "id", id, "error", err)
		}
	})
	return nil
}

// DeleteVector deletes a vector within the transaction.
// In-memory registry mutation is deferred to post-commit.
func (t *StoreTxn) DeleteVector(id uint64) bool {
	hasVec := t.store.deleteVectorInTxnDiskOnly(t.txn, id)
	if hasVec {
		st := t.store
		vid := id
		t.addPostCommit(func() { st.vectors.Delete(vid) })
	}
	return hasVec
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
// In-memory vector/dict mutations are deferred to post-commit.
func (t *StoreTxn) DeleteDocument(docKey string) error {
	return t.deleteDocumentInTxnDeferred(docKey, t.store.topicID.Load())
}

// DeleteDocumentWithTopic atomically deletes a document using a specific topicID.
func (t *StoreTxn) DeleteDocumentWithTopic(docKey string, topicID uint32) error {
	return t.deleteDocumentInTxnDeferred(docKey, topicID)
}

func (t *StoreTxn) deleteDocumentInTxnDeferred(docKey string, topicID uint32) error {
	id, err := t.store.dict.GetID(docKey)
	if err != nil {
		if err == dict.ErrNotFound {
			return nil
		}
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	// Delete content
	contentKey := keys.EncodeChunkKey(id)
	_ = t.txn.Delete(contentKey)

	// Delete vector from BadgerDB (defer in-memory delete)
	vecKey := keys.EncodeVectorFullKey(id)
	_ = t.txn.Delete(vecKey)
	if t.store.vectors.HasVector(id) {
		st := t.store
		vid := id
		t.addPostCommit(func() { st.vectors.Delete(vid) })
	}

	// Delete metadata facts
	packedID := keys.PackID(topicID, keys.UnpackLocalID(id))
	metadataPrefix := keys.EncodeTripleSPOPrefix(packedID, 0, 0)

	it := t.txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var deletedCount uint64
	for it.Seek(metadataPrefix); it.ValidForPrefix(metadataPrefix); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		s, p, o := keys.DecodeTripleKey(key)
		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, s, p, o)
		if err := t.txn.Delete(key); err != nil {
			return err
		}
		if err := t.txn.Delete(opsKey); err != nil {
			return err
		}
		deletedCount++
	}

	if deletedCount > 0 {
		t.factDelta -= int64(deletedCount)
	}

	// Delete dictionary entries (defer cache cleanup to post-commit)
	_ = t.store.dict.(*dict.Encoder).DeleteIDInTxnDiskOnly(t.txn, id, docKey)
	st := t.store
	dKey := docKey
	t.addPostCommit(func() {
		st.dict.(*dict.Encoder).RemoveFromCaches(dKey, id)
	})

	return nil
}

// Exists checks if a fact exists within the transaction.
func (t *StoreTxn) Exists(s, p, o string) bool {
	return t.store.existsInTxn(t.txn, s, p, o)
}

// --- Internal helpers that operate on a given badger.Txn ---

func (m *MEBStore) scanWithTxn(ctx context.Context, txn *badger.Txn, s, p, o string, topicID uint32) iterSeq2FactError {
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
		ctx:      ctx,
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

		scanCtx := opts.ctx
		if scanCtx == nil {
			scanCtx = context.Background()
		}

		for it.Seek(opts.strategy.prefix); it.ValidForPrefix(opts.strategy.prefix); it.Next() {
			select {
			case <-scanCtx.Done():
				yield(Fact{}, scanCtx.Err())
				return
			default:
			}

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
	_, err := m.addFactBatchInTxnCounted(txn, facts, topicID)
	return err
}

// addFactBatchInTxnCounted writes facts into the given transaction and returns the
// count of successfully inserted facts. The caller is responsible for updating the
// store's numFacts counter.
func (m *MEBStore) addFactBatchInTxnCounted(txn *badger.Txn, facts []Fact, topicID uint32) (int, error) {
	if err := validateFacts(facts); err != nil {
		return 0, fmt.Errorf("batch validation failed: %w", err)
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
		return 0, fmt.Errorf("failed to encode strings: %w", err)
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
				return 0, fmt.Errorf("failed to encode object for fact %d: %w", i, err)
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
			return 0, fmt.Errorf("failed to set SPO key for fact %d: %w", i, err)
		}

		opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, sID, pID, oID)
		if err := txn.Set(opsKey, value); err != nil {
			return 0, fmt.Errorf("failed to set OPS key for fact %d: %w", i, err)
		}
	}

	return len(facts), nil
}

func (m *MEBStore) deleteFactsBySubjectInTxn(txn *badger.Txn, subject string) (int, error) {
	sID, err := m.dict.GetID(subject)
	if err != nil {
		return 0, nil
	}

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
				return 0, err
			}
		}
	}

	it.Close()

	if err := flushBatch(); err != nil {
		return 0, err
	}

	return totalDeleted, nil
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

// deleteVectorInTxnDiskOnly deletes only from Badger, leaving in-memory registry intact.
// Used from StoreTxn.DeleteVector which defers registry mutation to post-commit.
func (m *MEBStore) deleteVectorInTxnDiskOnly(txn *badger.Txn, id uint64) bool {
	if !m.vectors.HasVector(id) {
		return false
	}

	vecKey := keys.EncodeVectorFullKey(id)
	if err := txn.Delete(vecKey); err != nil {
		return false
	}
	return true
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
