package meb

import (
	"fmt"

	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/vector"

	"github.com/dgraph-io/badger/v4"
)

// TripleKey identifies a triple for point lookup. Unlike a scan (which matches
// by prefix/wildcard), every component must be fully bound. The Object may be
// any value the store can encode (string, bool, int32, float32, or a
// dictionary-backed scalar).
type TripleKey struct {
	Subject   string
	Predicate string
	Object    any
}

// TripleResult carries the outcome of a single GetTriples lookup.
type TripleResult struct {
	Key   TripleKey
	Found bool
	Fact  Fact
}

// VectorResult carries the outcome of a single GetVectors lookup. The vector is
// the lossy dequantized reconstruction of the stored block-quantized hybrid
// data (matches what search operates on), not the original input vector.
type VectorResult struct {
	ID    uint64
	Found bool
	Vec   []float32
}

// GetTriples performs a batched point lookup for the given triple keys, resolving
// all dictionary IDs within a single read transaction. The result slice has the
// same length as keys, in the same order; Found is false for any key that does
// not resolve (unknown subject/predicate/object) or has no matching fact.
func (m *MEBStore) GetTriples(triples []TripleKey) ([]TripleResult, error) {
	if len(triples) == 0 {
		return nil, nil
	}

	topic := m.topicID.Load()
	results := make([]TripleResult, len(triples))
	spoKeys := make([][]byte, len(triples))

	getID := func(s string) (uint64, bool) {
		id, err := m.dict.GetID(s)
		if err != nil {
			return 0, false
		}
		return id, true
	}

	// Resolve every key to its topic-packed SPO key. Unresolvable keys stay nil
	// and are reported as not found.
	for i, k := range triples {
		results[i].Key = k
		if k.Subject == "" || k.Predicate == "" || k.Object == nil {
			continue
		}
		sLocal, ok := getID(k.Subject)
		if !ok {
			continue
		}
		pID, ok := getID(k.Predicate)
		if !ok {
			continue
		}
		oPacked, ok := m.packedObjectID(topic, k.Object, getID)
		if !ok {
			continue
		}
		sID := keys.PackID(topic, keys.UnpackLocalID(sLocal))
		spoKeys[i] = keys.EncodeTripleKey(keys.TripleSPOPrefix, sID, pID, oPacked)
	}

	err := m.withReadTxn(func(txn *badger.Txn) error {
		for i := range spoKeys {
			if spoKeys[i] == nil {
				continue
			}
			_, err := txn.Get(spoKeys[i])
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return fmt.Errorf("GetTriples: failed to look up key %d: %w", i, err)
			}
			results[i].Found = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := m.resolveTripleResults(results, spoKeys); err != nil {
		return nil, err
	}
	return results, nil
}

// packedObjectID encodes an Object value into its topic-packed dictionary ID,
// or its inline ID for inline primitives (bool, int32, float32). It returns
// ok=false when a dictionary-backed object cannot be resolved. Unlike
// encodeObject it uses GetID (never creating entries), which is what a
// read-only point lookup requires.
func (m *MEBStore) packedObjectID(topic uint32, obj any, getID func(string) (uint64, bool)) (uint64, bool) {
	switch v := obj.(type) {
	case string:
		id, ok := getID(v)
		if !ok {
			return 0, false
		}
		return keys.PackID(topic, keys.UnpackLocalID(id)), true
	case bool:
		return keys.PackInlineBool(v), true
	case int32:
		return keys.PackInlineInt32(v), true
	case float32:
		return keys.PackInlineFloat32(v), true
	case int:
		return m.packedDictObject(topic, fmt.Sprintf("%d", v), getID)
	case int64:
		return m.packedDictObject(topic, fmt.Sprintf("%d", v), getID)
	case float64:
		// %.17g preserves full float64 precision, mirroring encodeObject.
		return m.packedDictObject(topic, fmt.Sprintf("%.17g", v), getID)
	default:
		return m.packedDictObject(topic, fmt.Sprintf("%v", v), getID)
	}
}

func (m *MEBStore) packedDictObject(topic uint32, str string, getID func(string) (uint64, bool)) (uint64, bool) {
	id, ok := getID(str)
	if !ok {
		return 0, false
	}
	return keys.PackID(topic, keys.UnpackLocalID(id)), true
}

// resolveTripleResults batch-resolves the subject/predicate/object dictionary
// IDs of every found triple back to strings, using a single GetStrings call.
func (m *MEBStore) resolveTripleResults(results []TripleResult, spoKeys [][]byte) error {
	idSet := make(map[uint64]struct{})
	var ids []uint64
	addID := func(id uint64) {
		if _, ok := idSet[id]; !ok {
			idSet[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	for i := range results {
		if !results[i].Found {
			continue
		}
		sID, pID, oID := keys.DecodeTripleKey(spoKeys[i])
		addID(keys.UnpackLocalID(sID))
		addID(pID)
		if !keys.IsInline(oID) {
			addID(keys.UnpackLocalID(oID))
		}
	}

	resolved := make(map[uint64]string, len(ids))
	if len(ids) > 0 {
		strs, err := m.dict.GetStrings(ids)
		if err != nil {
			return fmt.Errorf("GetTriples: failed to resolve dictionary strings: %w", err)
		}
		for j, id := range ids {
			resolved[id] = strs[j]
		}
	}

	for i := range results {
		if !results[i].Found {
			continue
		}
		sID, pID, oID := keys.DecodeTripleKey(spoKeys[i])
		var object any
		if keys.IsInline(oID) {
			object = decodeInlineID(oID)
		} else {
			object = resolved[keys.UnpackLocalID(oID)]
		}
		results[i].Fact = Fact{
			Subject:   resolved[keys.UnpackLocalID(sID)],
			Predicate: resolved[pID],
			Object:    object,
		}
	}
	return nil
}

// GetVectors performs a batched read of the stored vectors for the given IDs,
// dequantizing each within a single read transaction. Missing IDs are reported
// with Found=false.
func (m *MEBStore) GetVectors(ids []uint64) ([]VectorResult, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	fullDim := m.vectors.FullDim()
	cfg := m.vectors.HybridConfig()
	if cfg == nil {
		cfg = vector.DefaultHybridConfig()
	}

	results := make([]VectorResult, len(ids))
	for i, id := range ids {
		results[i].ID = id
	}

	err := m.withReadTxn(func(txn *badger.Txn) error {
		for i, id := range ids {
			item, err := txn.Get(keys.EncodeVectorFullKey(id))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return fmt.Errorf("GetVectors: failed to read vector %d: %w", id, err)
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("GetVectors: failed to copy vector %d: %w", id, err)
			}
			// Stored layout is [semanticHash:1][hybridData...]; DequantizeHybrid
			// expects the data without the leading hash byte.
			hybrid := val
			if len(val) > 1 {
				hybrid = val[1:]
			}
			// Guard against malformed values: DequantizeHybrid decodes fixed
			// strides and would panic on a short slice. QuantizeHybrid wrote
			// HybridVectorSize(nextPow2(dim)) bytes, so require at least that.
			if len(hybrid) < vector.HybridVectorSize(pow2Ceil(fullDim), cfg) {
				continue
			}
			results[i].Vec = vector.DequantizeHybrid(hybrid, fullDim, cfg)
			results[i].Found = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// BatchWriter accumulates bulk triple writes and commits them through a single
// BadgerDB WriteBatch, amortizing fsync/commit overhead across many facts. It is
// intended for bulk/A^-style imports. Facts must be added via AddFacts; the
// batch is committed with Flush (or discarded with Cancel). A BatchWriter is
// single-goroutine and must not be reused after Flush or Cancel.
type BatchWriter struct {
	store       *MEBStore
	wb          *badger.WriteBatch
	seen        map[string]struct{}
	insertCount uint64
	closed      bool
}

// NewBatchWriter opens a new bulk writer against the store's default topic.
// Callers must either Flush or Cancel to release the underlying WriteBatch.
func (m *MEBStore) NewBatchWriter() *BatchWriter {
	return &BatchWriter{
		store: m,
		wb:    m.db.NewWriteBatch(),
		seen:  make(map[string]struct{}),
	}
}

// AddFacts queues the given facts for the batch. Facts whose triple already
// exists in the store (or was added earlier in this batch) are skipped and do
// not affect Flush's count, matching the transaction path's numFacts semantics.
func (bw *BatchWriter) AddFacts(facts []Fact) error {
	if bw.closed {
		return ErrStoreClosed
	}
	if bw.store.config.ReadOnly {
		return fmt.Errorf("%w: cannot write facts via batch", ErrStoreReadOnly)
	}
	if err := validateFacts(facts); err != nil {
		return err
	}

	factRefs, unique := collectStringRefs(facts)
	ids, err := bw.store.dict.GetIDs(unique)
	if err != nil {
		return fmt.Errorf("AddFacts: failed to encode strings: %w", err)
	}

	fks, err := bw.store.encodeFactKeys(facts, factRefs, ids)
	if err != nil {
		return fmt.Errorf("AddFacts: %w", err)
	}

	// Drop triples already added earlier in this batch.
	var pendings []factKey
	for _, fk := range fks {
		if _, ok := bw.seen[fk.dedup]; ok {
			continue
		}
		bw.seen[fk.dedup] = struct{}{}
		pendings = append(pendings, fk)
	}
	if len(pendings) == 0 {
		return nil
	}

	// Count only triples not already present, so numFacts stays exact.
	var insert uint64
	err = bw.store.withReadTxn(func(txn *badger.Txn) error {
		for _, fk := range pendings {
			_, err := txn.Get(fk.spo)
			if err == badger.ErrKeyNotFound {
				insert++
				continue
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("AddFacts: failed to check existing facts: %w", err)
	}

	for _, fk := range pendings {
		if err := bw.wb.Set(fk.spo, fk.value); err != nil {
			return fmt.Errorf("AddFacts: failed to set SPO key: %w", err)
		}
		if err := bw.wb.Set(fk.ops, fk.value); err != nil {
			return fmt.Errorf("AddFacts: failed to set OPS key: %w", err)
		}
	}
	bw.insertCount += insert
	return nil
}

// Flush commits the buffered writes, updates the store fact count with the
// number of newly inserted triples, and closes the batch. Flush may be called
// only once. Like the transaction path, it relies on Badger durability and does
// not write the WAL (see BatchWriter doc on AddFacts).
func (bw *BatchWriter) Flush() error {
	if bw.closed {
		return ErrStoreClosed
	}
	bw.closed = true
	if err := bw.wb.Flush(); err != nil {
		return fmt.Errorf("BatchWriter.Flush: %w", err)
	}
	if bw.insertCount > 0 {
		// Mirror addFactBatchInternal's post-flush bookkeeping so the fact
		// count, stats persistence, and auto-GC heuristics stay consistent.
		bw.store.numFacts.Add(bw.insertCount)
		bw.store.persistStatsIfNeeded(bw.insertCount)
		if bw.store.config.EnableAutoGC {
			bw.store.factsSinceGC.Add(bw.insertCount)
			bw.store.triggerAutoGC()
		}
	}
	return nil
}

// Cancel discards the batch without committing, releasing the WriteBatch.
func (bw *BatchWriter) Cancel() {
	if bw.closed {
		return
	}
	bw.closed = true
	bw.wb.Cancel()
}

// Count returns the number of new facts queued in this batch so far (i.e. those
// that will be reflected in the store's fact count after Flush).
func (bw *BatchWriter) Count() uint64 {
	return bw.insertCount
}
