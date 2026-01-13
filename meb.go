package meb

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/predicates"
	"github.com/duynguyendang/meb/store"
	"github.com/duynguyendang/meb/vector"
	"github.com/google/mangle/ast"
	"github.com/google/mangle/factstore"
	"github.com/klauspost/compress/s2"
)

// Fact represents a single Quad (Subject-Predicate-Object-Graph) in the knowledge base.
// This format supports multi-tenancy and RAG contexts by including a Graph identifier.
type Fact struct {
	Subject   string // The subject entity
	Predicate string // The predicate/relation
	Object    any    // The object value (can be string, int, float64, bool, etc.)
	Graph     string // The graph/context identifier. Defaults to "default" if empty.
}

// MEBStore implements both factstore.FactStore and store.KnowledgeStore interfaces.
// It uses BadgerDB for persistent storage and dictionary encoding for efficient operations.
type MEBStore struct {
	db     *badger.DB
	dictDB *badger.DB // Separate DB for dictionary
	dict   dict.Dictionary

	// Predicate tables
	predicates map[ast.PredicateSym]*predicates.PredicateTable

	// Configuration
	config *store.Config

	// Transaction pool for reads
	txPool *sync.Pool

	// Mutex for predicate table registration
	mu sync.RWMutex

	// numFacts tracks the total number of facts in RAM.
	// We use atomic.Uint64 for lock-free thread safety.
	// This value is persisted to disk only on graceful shutdown.
	numFacts atomic.Uint64

	// Vector registry for MRL vector search
	vectors *vector.VectorRegistry
}

// loadStats reads the counter from disk into RAM.
func (m *MEBStore) loadStats() error {
	return m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keys.KeyFactCount)
		if err == badger.ErrKeyNotFound {
			m.numFacts.Store(0)
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) >= 8 {
				count := binary.BigEndian.Uint64(val)
				m.numFacts.Store(count)
			}
			return nil
		})
	})
}

// saveStats writes the RAM counter to disk.
func (m *MEBStore) saveStats() error {
	return m.db.Update(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, m.numFacts.Load())
		return txn.Set(keys.KeyFactCount, buf)
	})
}

// NewMEBStore creates a new MEBStore with the given configuration.
func NewMEBStore(cfg *store.Config) (*MEBStore, error) {
	slog.Info("initializing MEB store",
		"dataDir", cfg.DataDir,
		"inMemory", cfg.InMemory,
		"blockCacheSize", cfg.BlockCacheSize,
		"indexCacheSize", cfg.IndexCacheSize,
		"numDictShards", cfg.NumDictShards,
	)

	// Validate configuration before proceeding
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Open BadgerDB for Facts
	db, err := store.OpenBadgerDB(cfg)
	if err != nil {
		slog.Error("failed to open BadgerDB", "error", err)
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	slog.Info("BadgerDB (Facts) opened successfully")

	// Open BadgerDB for Dictionary
	// Create a modified config for Dictionary DB
	dictCfg := *cfg // Copy config
	dictCfg.DataDir = cfg.DictDir
	dictCfg.SyncWrites = true // Enforce strict persistence for Dictionary
	// We might want different cache sizes for dict DB, but sharing config is a good start.
	// Maybe reduce block cache for dict as it's mostly key lookups?
	// For now, use same config but different dir.

	dictDB, err := store.OpenBadgerDB(&dictCfg)
	if err != nil {
		db.Close()
		slog.Error("failed to open Dictionary BadgerDB", "error", err)
		return nil, fmt.Errorf("failed to open Dictionary BadgerDB: %w", err)
	}
	slog.Info("BadgerDB (Dictionary) opened successfully")

	// Create dictionary encoder (sharded if configured)
	var dictEncoder dict.Dictionary
	if cfg.NumDictShards > 0 {
		slog.Info("creating sharded dictionary encoder", "shards", cfg.NumDictShards, "lruCacheSize", cfg.LRUCacheSize)
		dictEncoder, err = dict.NewShardedEncoder(dictDB, cfg.LRUCacheSize, cfg.NumDictShards)
		if err != nil {
			dictDB.Close()
			db.Close()
			return nil, fmt.Errorf("failed to create sharded dictionary encoder: %w", err)
		}
	} else {
		slog.Info("creating single-threaded dictionary encoder", "lruCacheSize", cfg.LRUCacheSize)
		dictEncoder, err = dict.NewEncoder(dictDB, cfg.LRUCacheSize)
		if err != nil {
			dictDB.Close()
			db.Close()
			return nil, fmt.Errorf("failed to create dictionary encoder: %w", err)
		}
	}

	m := &MEBStore{
		db:         db,
		dictDB:     dictDB,
		dict:       dictEncoder,
		predicates: make(map[ast.PredicateSym]*predicates.PredicateTable),
		config:     cfg,
		txPool: &sync.Pool{
			New: func() interface{} {
				return db.NewTransaction(false)
			},
		},
		vectors: vector.NewRegistry(db),
	}

	// Load vector snapshot from disk
	if err := m.vectors.LoadSnapshot(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load vector snapshot: %w", err)
	}

	// Load fact count stats from disk
	if err := m.loadStats(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load stats: %w", err)
	}

	// Register default predicates (triples)
	m.registerDefaultPredicates()

	slog.Info("MEB store initialized successfully", "factCount", m.numFacts.Load())
	return m, nil
}

// registerDefaultPredicates registers the built-in predicates.
func (m *MEBStore) registerDefaultPredicates() {
	// Register "triples" predicate for subject-predicate-object relationships
	triplesPred := ast.PredicateSym{Symbol: "triples", Arity: 3}
	m.predicates[triplesPred] = predicates.NewPredicateTable(m.db, m.dict, triplesPred, keys.SPOPrefix)
}

// newTxn gets a transaction from the pool or creates a new read-only transaction.
func (m *MEBStore) newTxn() *badger.Txn {
	return m.txPool.Get().(*badger.Txn)
}

// releaseTxn returns a transaction to the pool (for read-only transactions).
func (m *MEBStore) releaseTxn(txn *badger.Txn) {
	txn.Discard()
	m.txPool.Put(txn)
}

// === factstore.FactStore implementation ===

// GetFacts streams facts matching the given atom using the callback.
// It implements streaming semantics - never loads all results into memory.
func (m *MEBStore) GetFacts(atom ast.Atom, callback func(ast.Atom) error) error {
	// Convert atom arguments to Scan parameters
	var s, p, o, g string

	// For the "triples" predicate, the predicate is actually stored as part of the quad
	// The Datalog syntax ?triples(S, P, O) maps to the quad store format
	// where P is the actual predicate from the fact

	// Extract subject (first arg)
	if len(atom.Args) > 0 {
		if constTerm, ok := atom.Args[0].(ast.Constant); ok {
			s = constTerm.Symbol
		}
	}

	// Extract predicate (second arg for "triples" predicate, otherwise from atom.Predicate)
	if atom.Predicate.Symbol == "triples" && len(atom.Args) > 1 {
		// For ?triples(S, P, O), the predicate is the second argument
		if constTerm, ok := atom.Args[1].(ast.Constant); ok {
			p = constTerm.Symbol
		}
	} else {
		// For other predicates, use the atom's predicate symbol
		p = atom.Predicate.Symbol
	}

	// Extract object (third arg for "triples", otherwise second arg)
	var objectIndex int
	if atom.Predicate.Symbol == "triples" {
		objectIndex = 2 // Third argument
	} else {
		objectIndex = 1 // Second argument
	}

	if len(atom.Args) > objectIndex {
		if constTerm, ok := atom.Args[objectIndex].(ast.Constant); ok {
			o = constTerm.Symbol
		}
	}

	// Check for full table scan (no bound arguments)
	if s == "" && p == "" && o == "" && g == "" {
		return fmt.Errorf("full table scan not allowed")
	}

	// Note: Graph is not currently supported in the Datalog API

	// Use Scan to find matching facts
	for fact, err := range m.Scan(s, p, o, g) {
		if err != nil {
			return err
		}

		// Convert Fact back to ast.Atom format
		resultArgs := make([]ast.BaseTerm, 3)
		resultArgs[0] = ast.Constant{Type: ast.StringType, Symbol: fact.Subject}
		resultArgs[1] = ast.Constant{Type: ast.StringType, Symbol: fact.Predicate}

		// Object is stored as string in dictionary
		objectStr, ok := fact.Object.(string)
		if !ok {
			objectStr = fmt.Sprintf("%v", fact.Object)
		}
		resultArgs[2] = ast.Constant{Type: ast.StringType, Symbol: objectStr}

		resultAtom := ast.Atom{
			Predicate: atom.Predicate,
			Args:      resultArgs,
		}

		if err := callback(resultAtom); err != nil {
			return err
		}
	}

	return nil
}

// Add adds a fact to the store and returns true if it didn't exist before.
func (m *MEBStore) Add(atom ast.Atom) bool {
	// Check if already exists
	if m.Contains(atom) {
		return false
	}

	// For the "triples" predicate, extract the actual predicate and object from arguments
	var subject, predicate, object string
	var graph string = "default" // Default graph

	if atom.Predicate.Symbol == "triples" && len(atom.Args) >= 3 {
		// ?triples(S, P, O) format
		if constTerm, ok := atom.Args[0].(ast.Constant); ok {
			subject = constTerm.Symbol
		}
		if constTerm, ok := atom.Args[1].(ast.Constant); ok {
			predicate = constTerm.Symbol
		}
		if constTerm, ok := atom.Args[2].(ast.Constant); ok {
			object = constTerm.Symbol
		}
	} else {
		// Other predicates: use atom.Predicate as the predicate
		// This is for backward compatibility with non-triples predicates
		if len(atom.Args) >= 2 {
			if constTerm, ok := atom.Args[0].(ast.Constant); ok {
				subject = constTerm.Symbol
			}
			predicate = atom.Predicate.Symbol
			if constTerm, ok := atom.Args[1].(ast.Constant); ok {
				object = constTerm.Symbol
			}
		}
	}

	// Add fact using the quad store format
	fact := Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Graph:     graph,
	}

	err := m.AddFactBatch([]Fact{fact})
	if err != nil {
		return false
	}

	return true
}

// Contains returns true if the given atom is present in the store.
func (m *MEBStore) Contains(atom ast.Atom) bool {
	found := false
	m.GetFacts(atom, func(ast.Atom) error {
		found = true
		// Return error to stop iteration
		return fmt.Errorf("found")
	})

	return found
}

// ListPredicates lists all predicates available in the store.
func (m *MEBStore) ListPredicates() []ast.PredicateSym {
	m.mu.RLock()
	defer m.mu.RUnlock()

	preds := make([]ast.PredicateSym, 0, len(m.predicates))
	for pred := range m.predicates {
		preds = append(preds, pred)
	}
	return preds
}

// Merge merges contents of the given store into this store.
func (m *MEBStore) Merge(other factstore.ReadOnlyFactStore) error {
	for _, pred := range other.ListPredicates() {
		if err := other.GetFacts(ast.NewQuery(pred), func(atom ast.Atom) error {
			m.Add(atom)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// === store.KnowledgeStore implementation ===

// AddFact inserts a single fact into the knowledge base.
func (m *MEBStore) AddFact(fact Fact) error {
	// Use AddFactBatch for consistency with quad store format
	return m.AddFactBatch([]Fact{fact})
}

// AddFactBatch inserts multiple facts in a single operation using quad indices.
// Uses batch dictionary encoding for optimal performance.
// Populates 3 indices: SPOG (forward), POSG (reverse), GSPO (graph lifecycle).
func (m *MEBStore) AddFactBatch(facts []Fact) error {
	// 1. Collect all UNIQUE strings that need encoding
	type stringRef struct {
		index int  // Index in uniqueStrings
		isObj bool // True if this is an object string
	}
	factStringRefs := make([][]stringRef, len(facts))
	uniqueStringsMap := make(map[string]int) // string -> index in uniqueStrings
	var uniqueStrings []string

	for i, fact := range facts {
		// Normalize graph to "default" if empty
		graph := fact.Graph
		if graph == "" {
			graph = "default"
		}

		// Process Subject
		if _, ok := uniqueStringsMap[fact.Subject]; !ok {
			uniqueStringsMap[fact.Subject] = len(uniqueStrings)
			uniqueStrings = append(uniqueStrings, fact.Subject)
		}
		factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[fact.Subject], isObj: false})

		// Process Predicate
		if _, ok := uniqueStringsMap[fact.Predicate]; !ok {
			uniqueStringsMap[fact.Predicate] = len(uniqueStrings)
			uniqueStrings = append(uniqueStrings, fact.Predicate)
		}
		factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[fact.Predicate], isObj: false})

		// Process Graph
		if _, ok := uniqueStringsMap[graph]; !ok {
			uniqueStringsMap[graph] = len(uniqueStrings)
			uniqueStrings = append(uniqueStrings, graph)
		}
		factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[graph], isObj: false})

		// Process Object if it's a string
		if s, ok := fact.Object.(string); ok {
			if _, ok := uniqueStringsMap[s]; !ok {
				uniqueStringsMap[s] = len(uniqueStrings)
				uniqueStrings = append(uniqueStrings, s)
			}
			factStringRefs[i] = append(factStringRefs[i], stringRef{index: uniqueStringsMap[s], isObj: true})
		}
	}

	// 2. Batch encode all UNIQUE strings to IDs (single call, minimal locking)
	ids, err := m.dict.GetIDs(uniqueStrings)
	if err != nil {
		return err
	}

	// 3. Build BadgerDB batch using pre-encoded IDs (pure RAM operations)
	batch := m.db.NewWriteBatch()
	defer batch.Cancel()

	for i, fact := range facts {
		// Normalize graph to "default" if empty
		graph := fact.Graph
		if graph == "" {
			graph = "default"
		}

		// Get IDs for Subject, Predicate, Graph from refs
		sID := ids[factStringRefs[i][0].index]
		pID := ids[factStringRefs[i][1].index]
		// gID := ids[factStringRefs[i][2].index] // Graph ID unused in 24-byte key mode

		// Handle Object (could be string or other type)
		var oID uint64

		if len(factStringRefs[i]) > 3 && factStringRefs[i][3].isObj {
			// Object is a string, use the ID from refs
			oID = ids[factStringRefs[i][3].index]
		} else {
			// Object is not a string, need to encode it
			_, oID, err = m.encodeObject(fact.Object)
			if err != nil {
				return err
			}
		}

		// Add to main index: SPO (25 bytes)
		spogKey := keys.EncodeSPOKey(sID, pID, oID)
		if err := batch.Set(spogKey, nil); err != nil {
			return err
		}

		// Add to inverse index: OPS (25 bytes)
		opsKey := keys.EncodeOPSKey(sID, pID, oID)
		if err := batch.Set(opsKey, nil); err != nil {
			return err
		}

		// Update fact count (zero-cost atomic operation)
		m.numFacts.Add(1)
	}

	return batch.Flush()
}

// GetMEBOptions returns BadgerDB options optimized for the host environment.
func GetMEBOptions(isReadOnly bool) badger.Options {
	opts := badger.DefaultOptions("")

	// Detect environment (simplified logic for now)
	// In a real scenario, this would check environment variables or runtime flags.
	// Default to "cloud_run" profile for safety (low memory) if not specified.

	opts.ReadOnly = isReadOnly
	opts.Logger = nil // Disable default logger to avoid noise

	// Profile: Cloud Run (Serving) - Low Memory
	// Optimization: bloom filters off for read-only if valid, small cache
	if isReadOnly {
		opts.IndexCacheSize = 256 << 20 // 256MB
		opts.BlockCacheSize = 64 << 20  // 64MB

	} else {
		// Profile: VM (Ingestion) - High Throughput
		opts.IndexCacheSize = 2 << 30 // 2GB
		opts.BlockCacheSize = 1 << 30 // 1GB
		opts.NumCompactors = 4
		opts.CompactL0OnClose = true
	}

	return opts
}

// DeleteGraph removes all facts belonging to the specified graph context.
// Uses the GSPO index for efficient O(N) deletion where N is the number of facts in the graph.
func (m *MEBStore) DeleteGraph(graph string) error {
	// Normalize graph name
	if graph == "" {
		graph = "default"
	}

	slog.Info("deleting graph", "graph", graph)

	// Get graph ID
	gID, err := m.dict.GetID(graph)
	if err != nil {
		// Graph doesn't exist, nothing to delete
		slog.Debug("graph not found, nothing to delete", "graph", graph)
		return nil
	}

	// First pass: collect all GSPO keys to delete
	txn := m.db.NewTransaction(false)

	prefix := keys.EncodeQuadGSPOPrefix(gID)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	type quadKeys struct {
		gspo []byte
		spog []byte
		posg []byte
	}
	var keysToDelete []quadKeys

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		// Decode the quad key
		s, p, o, g := keys.DecodeQuadKey(key)

		// Copy the GSPO key (make a new byte slice)
		gspoKey := make([]byte, len(key))
		copy(gspoKey, key)

		// Generate all three keys
		spogKey := keys.EncodeQuadKey(keys.QuadSPOGPrefix, s, p, o, g)
		posgKey := keys.EncodeQuadKey(keys.QuadPOSGPrefix, s, p, o, g)

		keysToDelete = append(keysToDelete, quadKeys{
			gspo: gspoKey,
			spog: spogKey,
			posg: posgKey,
		})
	}
	it.Close()
	txn.Discard()

	slog.Debug("collected keys for deletion", "count", len(keysToDelete))

	// Second pass: delete all keys in a write transaction
	deleteTxn := m.db.NewTransaction(true)

	for _, keys := range keysToDelete {
		if err := deleteTxn.Delete(keys.spog); err != nil {
			slog.Error("failed to delete SPOG key", "error", err)
			deleteTxn.Discard()
			return err
		}
		if err := deleteTxn.Delete(keys.posg); err != nil {
			slog.Error("failed to delete POSG key", "error", err)
			deleteTxn.Discard()
			return err
		}
		if err := deleteTxn.Delete(keys.gspo); err != nil {
			slog.Error("failed to delete GSPO key", "error", err)
			deleteTxn.Discard()
			return err
		}
		// Update fact count (zero-cost atomic operation)
		m.numFacts.Add(^uint64(0)) // Atomic decrement
	}

	// Commit the transaction
	if err := deleteTxn.Commit(); err != nil {
		slog.Error("failed to commit delete transaction", "error", err)
		return err
	}

	slog.Info("graph deleted successfully", "graph", graph, "factsDeleted", len(keysToDelete))
	return nil
}

// Scan returns an iterator over facts matching the pattern using Go 1.23 iter.Seq2.
// Empty string means wildcard (match all).
// Intelligently selects the best index (SPOG vs POSG) based on input arguments.
//
// Index Selection Strategy:
//   - If Graph is bound + Subject is bound -> Use SPOG (Prefix: G|S|P...)
//   - If Subject is bound -> Use SPOG (Prefix: S|P...)
//   - If Object is bound -> Use POSG (Prefix: P|O...)
//   - If Graph is bound (only) -> Use GSPO (Prefix: G|S...)
//   - Else -> Return empty iterator (requires at least one bound arg)
func (m *MEBStore) Scan(s, p, o, g string) iter.Seq2[Fact, error] {
	return m.ScanContext(context.Background(), s, p, o, g)
}

// ScanContext is like Scan but accepts a context for cancellation.
// optimized for 24-byte S|P|O keys.
func (m *MEBStore) ScanContext(ctx context.Context, s, p, o, g string) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		// Collect bound arguments
		var sID, pID, oID uint64
		var sBound, pBound, oBound bool
		var err error

		// Convert bound arguments to IDs
		if s != "" {
			sID, err = m.dict.GetID(s)
			if err != nil {
				return // Subject not found -> no facts
			}
			sBound = true
		}

		if p != "" {
			pID, err = m.dict.GetID(p)
			if err != nil {
				return // Predicate not found -> no facts
			}
			pBound = true
		}

		if o != "" {
			oID, err = m.dict.GetID(o)
			if err != nil {
				return // Object not found -> no facts
			}
			oBound = true
		}

		// Reject full table scans (no arguments bound)
		if !sBound && !pBound && !oBound {
			// If only G is bound (which we ignore in keys), we can't scan efficiently
			return
		}

		// Determine Index and Prefix
		var prefix []byte
		var useOPS bool

		if sBound {
			// Strategy: Use SPO index
			// Prefix: [S] or [S|P] (We can't do S|P|O efficiently if O is also bound but we want to iterate?
			//          Actually if S, P, O are ALL bound, we just check existence.
			//          For iteration, we usually have at least one unbound.
			//          If all bound, prefix scan works fine too (returns 1 item).
			prefix = keys.EncodeSPOPrefix(sID, pID)
		} else if oBound {
			// Strategy: Use OPS index
			// Prefix: [O] or [O|P]
			useOPS = true
			prefix = keys.EncodeOPSPrefix(oID, pID)
		} else {
			// Only P bound?
			// Defined in requirements: "Inverse Index (OPS)... Purpose: O(1) for Find all entities related to O".
			// If only P bound, we still can't scan efficiently without full scan?
			// Most triples stores don't optimize for (? P ?) without additional index (POS).
			// We only have SPO and OPS.
			// So we return empty or error?
			// Let's assume we return empty as per "Scan returns... efficiently".
			return
		}

		// Create read-only transaction
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		// Create iterator
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys

		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan using the prefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				yield(Fact{}, ctx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.TripleKeySize { // 25 bytes
				continue // Skip non-fact keys
			}

			// Decode the key
			var foundSID, foundPID, foundOID uint64
			if useOPS {
				foundSID, foundPID, foundOID = keys.DecodeOPSKey(key)
			} else {
				foundSID, foundPID, foundOID = keys.DecodeSPOKey(key)
			}

			// Validate with specific bindings (filter step)
			// (Prefix scan handles most, but if we did S|P prefix and O was also bound (existence check), we need to verify O)
			if sBound && foundSID != sID {
				continue
			}
			if pBound && foundPID != pID {
				continue
			}
			if oBound && foundOID != oID {
				continue
			}

			// Resolve Strings
			var subject, predicate, objectStr string

			// Subject
			if sBound {
				subject = s
			} else {
				var err error
				subject, err = m.dict.GetString(foundSID)
				if err != nil {
					yield(Fact{}, err)
					return
				}
			}

			// Predicate
			if pBound {
				predicate = p
			} else {
				var err error
				predicate, err = m.dict.GetString(foundPID)
				if err != nil {
					yield(Fact{}, err)
					return
				}
			}

			// Object
			if oBound {
				objectStr = o
			} else {
				var err error
				objectStr, err = m.dict.GetString(foundOID)
				if err != nil {
					yield(Fact{}, err)
					return
				}
			}

			// Build the Fact
			fact := Fact{
				Subject:   subject,
				Predicate: predicate,
				Object:    objectStr,
				Graph:     "default", // Graph info partially lost in dual-index mode, handled externally or assumed default
			}

			if !yield(fact, nil) {
				return
			}
		}
	}
}

// Query executes a Datalog query and returns results.
// This provides compatibility with the existing KnowledgeStore interface.
func (m *MEBStore) Query(ctx context.Context, query string) ([]map[string]any, error) {
	// Parse the query (simple format: ?predicate(arg1, arg2, ...))
	pred, args, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// Identify variable positions
	vars := make(map[int]string)
	for i, arg := range args {
		if strings.HasPrefix(arg, "?") || arg == "_" {
			vars[i] = arg
		}
	}

	// Build query atom
	atomArgs := make([]ast.BaseTerm, len(args))
	for i, arg := range args {
		if _, isVar := vars[i]; isVar {
			// Variable - use ast.Variable type
			atomArgs[i] = ast.Variable{}
		} else {
			// Constant - trim both single and double quotes
			atomArgs[i] = ast.Constant{Type: ast.StringType, Symbol: strings.Trim(arg, "'\"")}
		}
	}

	atom := ast.Atom{
		Predicate: ast.PredicateSym{Symbol: pred, Arity: len(args)},
		Args:      atomArgs,
	}

	// Execute query using streaming GetFacts
	var results []map[string]any
	err = m.GetFacts(atom, func(result ast.Atom) error {
		row := make(map[string]any)

		for i, arg := range result.Args {
			if _, isVar := vars[i]; isVar {
				varName := args[i]
				// Auto-number bare ? and _ variables by argument position
				if varName == "?" {
					varName = fmt.Sprintf("?%d", i)
				} else if varName == "_" {
					varName = fmt.Sprintf("_%d", i)
				}
				row[varName] = m.termToGoValue(arg)
			}
		}

		results = append(results, row)
		return nil
	})

	return results, err
}

// Reset clears the store by deleting all data.
func (m *MEBStore) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slog.Info("resetting store", "factCount", m.numFacts.Load())

	// Clear all data
	err := m.db.DropAll()
	if err != nil {
		slog.Error("failed to drop all data", "error", err)
		return err
	}

	// Reset fact count (atomic operation)
	m.numFacts.Store(0)

	slog.Info("store reset complete")
	return nil
}

// Close closes the store and releases resources.
func (m *MEBStore) Close() error {
	slog.Info("closing store", "factCount", m.numFacts.Load())

	// Save vector snapshot before closing
	if err := m.vectors.SaveSnapshot(); err != nil {
		slog.Error("failed to save vector snapshot", "error", err)
		return fmt.Errorf("failed to save vector snapshot: %w", err)
	}

	// Save fact count stats to disk
	if err := m.saveStats(); err != nil {
		slog.Error("failed to save stats", "error", err)
		return fmt.Errorf("failed to save stats: %w", err)
	}

	// Wait for vector operations to complete
	if err := m.vectors.Close(); err != nil {
		slog.Error("failed to close vectors", "error", err)
		return err
	}

	// Close dictionary
	if err := m.dict.Close(); err != nil {
		slog.Error("failed to close dictionary", "error", err)
		return err
	}

	// Close Dictionary BadgerDB
	if err := m.dictDB.Close(); err != nil {
		slog.Error("failed to close dictionary database", "error", err)
		// We still try to close the main DB even if this fails
	}

	// Close BadgerDB
	if err := m.db.Close(); err != nil {
		slog.Error("failed to close database", "error", err)
		return err
	}

	slog.Info("store closed successfully")
	return nil
}

// Count returns the total number of facts in the store.
// This is a zero-cost atomic read from memory.
func (m *MEBStore) Count() uint64 {
	return m.numFacts.Load()
}

// RecalculateStats forces a full DB scan to fix the fact counter.
// This is an expensive operation that should only be used if the counter
// is suspected to be out of sync (e.g., after an unclean shutdown).
// It scans the SPOG index and updates both the in-memory counter and disk.
func (m *MEBStore) RecalculateStats() (uint64, error) {
	slog.Info("recalculating stats (expensive operation)", "currentCount", m.numFacts.Load())

	var count uint64

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Key-only is much faster
		it := txn.NewIterator(opts)
		defer it.Close()

		// Count only primary SPOG keys
		prefix := []byte{keys.QuadSPOGPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	if err != nil {
		slog.Error("failed to recalculate stats", "error", err)
		return 0, fmt.Errorf("failed to recalculate stats: %w", err)
	}

	// Update RAM counter
	m.numFacts.Store(count)

	// Save to disk
	if err := m.saveStats(); err != nil {
		slog.Error("failed to save recalculated stats", "error", err)
		return 0, fmt.Errorf("failed to save recalculated stats: %w", err)
	}

	slog.Info("stats recalculated successfully", "newCount", count)
	return count, nil
}

// Vectors returns the vector registry for vector search operations.
func (m *MEBStore) Vectors() *vector.VectorRegistry {
	return m.vectors
}

// Find returns a new query builder for neuro-symbolic search.
// Example:
//
//	results, err := store.Find().
//	    SimilarTo(embedding).
//	    Where("author", "alice").
//	    Limit(5).
//	    Execute()
func (m *MEBStore) Find() *Builder {
	return NewBuilder(m)
}

// SetContent stores compressed content for a given ID.
// The content is compressed using S2 compression before storage.
func (m *MEBStore) SetContent(id uint64, data []byte) error {
	// Compress the data using S2
	compressed := s2.Encode(nil, data)

	// Create the key
	key := keys.EncodeChunkKey(id)

	// Store in BadgerDB
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, compressed)
	})
}

// GetContent retrieves and decompresses content for a given ID.
// Returns the original decompressed bytes.
// If the content is not found, returns nil with no error.
func (m *MEBStore) GetContent(id uint64) ([]byte, error) {
	key := keys.EncodeChunkKey(id)

	var data []byte
	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		// Key not found is not an error - content is optional
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	// Decompress the data
	decompressed, err := s2.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress content: %w", err)
	}

	// Handle edge case: if decompressed is nil but data was not empty, return empty slice
	if decompressed == nil && len(data) > 0 {
		return []byte{}, nil
	}

	return decompressed, nil
}

// AddDocument adds a complete document with vector, content, and metadata.
// This is a high-level helper that handles the full RAG pipeline.
func (m *MEBStore) AddDocument(docKey string, content []byte, vec []float32, metadata map[string]any) error {
	slog.Debug("adding document",
		"key", docKey,
		"contentSize", len(content),
		"vectorDim", len(vec),
		"metadataCount", len(metadata),
	)

	// 1. Get or create ID for the document
	id, err := m.dict.GetOrCreateID(docKey)
	if err != nil {
		slog.Error("failed to get document ID", "key", docKey, "error", err)
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	// 2. Store vector
	if err := m.vectors.Add(id, vec); err != nil {
		slog.Error("failed to add vector", "key", docKey, "error", err)
		return fmt.Errorf("failed to add vector: %w", err)
	}

	// 3. Store content (compressed)
	if err := m.SetContent(id, content); err != nil {
		slog.Error("failed to store content", "key", docKey, "error", err)
		return fmt.Errorf("failed to store content: %w", err)
	}

	// 4. Store metadata as facts (as quads: subject, predicate, object, graph)
	// Note: We use AddFact instead of AddFactBatch because metadata can have mixed types
	if metadata != nil && len(metadata) > 0 {
		for key, value := range metadata {
			fact := Fact{
				Subject:   docKey,
				Predicate: key,
				Object:    value,
				Graph:     "metadata",
			}
			if err := m.AddFact(fact); err != nil {
				slog.Error("failed to add metadata fact", "key", docKey, "predicate", key, "error", err)
				return fmt.Errorf("failed to add metadata fact for %s: %w", key, err)
			}
		}
	}

	slog.Debug("document added successfully", "key", docKey, "id", id)
	return nil
}

// === Helper methods ===

// encodeObject converts an object value to its dictionary ID and string representation.
// Handles various types including int, int64, float64, bool, and string.
func (m *MEBStore) encodeObject(obj any) (string, uint64, error) {
	switch v := obj.(type) {
	case string:
		return v, 0, nil // ID will be obtained from batch
	case int:
		objStr := fmt.Sprintf("%d", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		return objStr, oID, err
	case int64:
		objStr := fmt.Sprintf("%d", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		return objStr, oID, err
	case float64:
		objStr := fmt.Sprintf("%f", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		return objStr, oID, err
	case bool:
		objStr := fmt.Sprintf("%t", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		return objStr, oID, err
	default:
		objStr := fmt.Sprintf("%v", v)
		oID, err := m.dict.GetOrCreateID(objStr)
		return objStr, oID, err
	}
}

// factToAtom converts a Fact to a Mangle Atom.
func (m *MEBStore) factToAtom(fact Fact) (ast.Atom, error) {
	// Create args from Subject, Object, and Graph (not Predicate)
	args := []ast.BaseTerm{
		ast.String(fact.Subject),
		m.goValueToTerm(fact.Object),
		ast.String(fact.Graph),
	}
	return ast.NewAtom(fact.Predicate, args...), nil
}

// goValueToTerm converts a Go value to a Mangle term.
func (m *MEBStore) goValueToTerm(v any) ast.BaseTerm {
	switch val := v.(type) {
	case string:
		return ast.String(val)
	case int:
		return ast.Number(int64(val))
	case int64:
		return ast.Number(val)
	case float64:
		return ast.Float64(val)
	case bool:
		if val {
			return ast.String("true")
		}
		return ast.String("false")
	default:
		return ast.String(fmt.Sprintf("%v", val))
	}
}

// termToGoValue converts a Mangle term to a Go value.
func (m *MEBStore) termToGoValue(term ast.BaseTerm) any {
	switch t := term.(type) {
	case ast.Constant:
		switch t.Type {
		case ast.StringType:
			return t.Symbol
		case ast.NumberType:
			return t.NumValue
		case ast.Float64Type:
			return t.NumValue
		default:
			return t.Symbol
		}
	default:
		return fmt.Sprintf("%v", t)
	}
}

// parseQuery parses a simple Datalog query string.
// Format: ?predicate(arg1, arg2, ...)
func parseQuery(query string) (string, []string, error) {
	query = strings.TrimSpace(query)

	// Remove leading ?
	if strings.HasPrefix(query, "?") {
		query = query[1:]
	}

	// Find predicate and arguments
	start := strings.Index(query, "(")
	end := strings.LastIndex(query, ")")

	if start == -1 || end == -1 || start >= end {
		return "", nil, fmt.Errorf("invalid query format: %s", query)
	}

	predicate := strings.TrimSpace(query[:start])
	argsStr := strings.TrimSpace(query[start+1 : end])

	var args []string
	if argsStr != "" {
		args = splitArgs(argsStr)
	}

	return predicate, args, nil
}

// splitArgs splits argument string by comma, handling nested structures.
func splitArgs(s string) []string {
	var args []string
	var current strings.Builder
	depth := 0

	for _, ch := range s {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args
}

// === Generic Helper Functions ===
// These functions provide type-safe access to Fact objects using Go generics.

// Value safely casts the Fact object to type T.
// Returns the value and true if successful, zero value and false otherwise.
//
// Example:
//
//	for f, err := range store.Scan("Alice", "knows", "", "") {
//	    if err != nil { panic(err) }
//	    name, ok := Value[string](f)
//	    if ok {
//	        fmt.Printf("Alice knows %s\n", name)
//	    }
//	}
func Value[T any](f Fact) (T, bool) {
	var zero T
	if f.Object == nil {
		return zero, false
	}

	v, ok := f.Object.(T)
	if !ok {
		return zero, false
	}

	return v, true
}

// MustValue casts the Fact object to type T or panics.
// Useful for tests and scripts where you're certain of the type.
//
// Example:
//
//	for f, err := range store.Scan("Alice", "age", "", "") {
//	    if err != nil { panic(err) }
//	    age := MustValue[int](f)
//	    fmt.Printf("Alice is %d years old\n", age)
//	}
func MustValue[T any](f Fact) T {
	v, ok := Value[T](f)
	if !ok {
		panic(fmt.Sprintf("failed to cast %v to %T", f.Object, *new(T)))
	}
	return v
}

// ValueOrDefault casts the Fact object to type T.
// Returns the value if successful, otherwise returns the provided default value.
//
// Example:
//
//	for f, err := range store.Scan("Alice", "age", "", "") {
//	    if err != nil { panic(err) }
//	    age := ValueOrDefault(f, 0)
//	    fmt.Printf("Alice is %d years old\n", age)
//	}
func ValueOrDefault[T any](f Fact, defaultVal T) T {
	if v, ok := Value[T](f); ok {
		return v
	}
	return defaultVal
}

// Collect collects all facts from an iterator into a slice.
// Stops on first error and returns it.
//
// Example:
//
//	facts, err := Collect(store.Scan("Alice", "", "", ""))
//	if err != nil { panic(err) }
//	fmt.Printf("Found %d facts about Alice\n", len(facts))
func Collect(seq iter.Seq2[Fact, error]) ([]Fact, error) {
	facts := make([]Fact, 0)

	for f, err := range seq {
		if err != nil {
			return nil, err
		}
		facts = append(facts, f)
	}

	return facts, nil
}

// Filter creates a new iterator that only yields facts matching the predicate.
//
// Example:
//
//	// Find all people Alice knows who are adults
//	filtered := Filter(
//	    store.Scan("Alice", "knows", "", ""),
//	    func(f Fact) bool {
//	        // Check if this person is an adult
//	        ageFacts, _ := Collect(store.Scan(f.Object.(string), "age", "", ""))
//	        for _, af := range ageFacts {
//	            if age, ok := Value[int](af); ok && age >= 18 {
//	                return true
//	            }
//	        }
//	        return false
//	    },
//	)
func Filter(seq iter.Seq2[Fact, error], pred func(Fact) bool) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		for f, err := range seq {
			if err != nil {
				yield(Fact{}, err)
				return
			}
			if pred(f) {
				if !yield(f, nil) {
					return
				}
			}
		}
	}
}

// Map transforms each fact using the provided function.
//
// Example:
//
//	mapped := Map(store.Scan("Alice", "knows", "", ""), func(f Fact) (string, error) {
//	    name, ok := Value[string](f)
//	    if !ok {
//	        return "", fmt.Errorf("object is not a string")
//	    }
//	    return name, nil
//	})
//
// Map transforms a sequence of facts into a sequence of values of type T.
func Map[T any](seq iter.Seq2[Fact, error], fn func(Fact) (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for f, err := range seq {
			if err != nil {
				yield(*new(T), err)
				return
			}
			result, err := fn(f)
			if err != nil {
				yield(*new(T), err)
				return
			}
			if !yield(result, nil) {
				return
			}
		}
	}
}

// First returns the first fact from the iterator, or an error if none exists.
//
// Example:
//
//	fact, err := First(store.Scan("Alice", "knows", "", ""))
//	if err != nil { panic(err) }
//	fmt.Printf("First person Alice knows: %v\n", fact.Object)
func First(seq iter.Seq2[Fact, error]) (Fact, error) {
	for f, err := range seq {
		if err != nil {
			return Fact{}, err
		}
		return f, nil
	}
	return Fact{}, fmt.Errorf("no facts found")
}

// Count counts the number of facts in the iterator.
//
// Example:
//
//	count, err := Count(store.Scan("Alice", "", "", ""))
//	if err != nil { panic(err) }
//	fmt.Printf("Alice has %d facts\n", count)
func Count(seq iter.Seq2[Fact, error]) (int, error) {
	count := 0
	for _, err := range seq {
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}
