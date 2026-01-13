package meb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
	"github.com/google/mangle/ast"
)

// === store.KnowledgeStore implementation ===

// AddFact inserts a single fact into the knowledge base.
func (m *MEBStore) AddFact(fact Fact) error {
	// Validate the fact
	if err := validateFact(fact); err != nil {
		return fmt.Errorf("failed to add fact: %w", err)
	}

	// Use AddFactBatch for consistency with quad store format
	return m.AddFactBatch([]Fact{fact})
}

// AddFactBatch inserts multiple facts in a single operation using quad indices.
// Uses batch dictionary encoding for optimal performance.
// Populates 2 indices: SPO (forward), OPS (reverse).
func (m *MEBStore) AddFactBatch(facts []Fact) error {
	// Validate all facts first
	if err := validateFacts(facts); err != nil {
		return fmt.Errorf("batch validation failed: %w", err)
	}

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
		graph := normalizeGraph(fact.Graph)

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
		return fmt.Errorf("failed to encode strings: %w", err)
	}

	// 3. Build BadgerDB batch using pre-encoded IDs (pure RAM operations)
	batch := m.db.NewWriteBatch()
	defer batch.Cancel()

	for i, fact := range facts {
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
				return fmt.Errorf("failed to encode object for fact %d: %w", i, err)
			}
		}

		// Add to main index: SPO (25 bytes)
		spogKey := keys.EncodeSPOKey(sID, pID, oID)
		if err := batch.Set(spogKey, nil); err != nil {
			return fmt.Errorf("failed to set SPO key for fact %d: %w", i, err)
		}

		// Add to inverse index: OPS (25 bytes)
		opsKey := keys.EncodeOPSKey(sID, pID, oID)
		if err := batch.Set(opsKey, nil); err != nil {
			return fmt.Errorf("failed to set OPS key for fact %d: %w", i, err)
		}

		// Update fact count (zero-cost atomic operation)
		m.numFacts.Add(1)
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return nil
}

// DeleteGraph removes all facts belonging to the specified graph context.
// Uses batched deletion for memory efficiency with graphs containing millions of facts.
func (m *MEBStore) DeleteGraph(graph string) error {
	// Normalize graph name
	graph = normalizeGraph(graph)

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

	const maxDeleteBatchSize = 1000
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

	if len(keysToDelete) == 0 {
		slog.Info("no facts found in graph", "graph", graph)
		return nil
	}

	// Second pass: delete all keys in batches
	totalDeleted := 0
	for i := 0; i < len(keysToDelete); i += maxDeleteBatchSize {
		end := i + maxDeleteBatchSize
		if end > len(keysToDelete) {
			end = len(keysToDelete)
		}

		batch := keysToDelete[i:end]

		deleteTxn := m.db.NewTransaction(true)
		for _, keys := range batch {
			if err := deleteTxn.Delete(keys.spog); err != nil {
				slog.Error("failed to delete SPOG key", "error", err)
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete SPOG key: %w", err)
			}
			if err := deleteTxn.Delete(keys.posg); err != nil {
				slog.Error("failed to delete POSG key", "error", err)
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete POSG key: %w", err)
			}
			if err := deleteTxn.Delete(keys.gspo); err != nil {
				slog.Error("failed to delete GSPO key", "error", err)
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete GSPO key: %w", err)
			}
			// Update fact count (zero-cost atomic operation)
			m.numFacts.Add(^uint64(0)) // Atomic decrement
		}

		// Commit the transaction
		if err := deleteTxn.Commit(); err != nil {
			slog.Error("failed to commit delete transaction", "error", err)
			return fmt.Errorf("failed to commit delete batch: %w", err)
		}

		totalDeleted += len(batch)
	}

	slog.Info("graph deleted successfully", "graph", graph, "factsDeleted", totalDeleted)
	return nil
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
