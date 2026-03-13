package meb

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
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
// Populates 3 indices: SPOG (Subject-Predicate-Object-Graph),
//
//	OPSG (Object-Predicate-Subject-Graph),
//	GSPO (Graph-Subject-Predicate-Object).
//
// All keys are 33-byte quad format for multi-tenant support.
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
		gID := ids[factStringRefs[i][2].index] // Graph ID now used in 33-byte quad key mode

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

		// Add to main index: SPOG (33 bytes) - Subject-Predicate-Object-Graph
		spogKey := keys.EncodeQuadKey(keys.QuadSPOGPrefix, sID, pID, oID, gID)
		if err := batch.Set(spogKey, nil); err != nil {
			return fmt.Errorf("failed to set SPOG key for fact %d: %w", i, err)
		}

		// Add to inverse index: OPSG (33 bytes) - Object-Predicate-Subject-Graph
		// EncodeQuadKey handles the physical reordering internally based on prefix
		opsgKey := keys.EncodeQuadKey(keys.QuadOPSGPrefix, sID, pID, oID, gID)
		if err := batch.Set(opsgKey, nil); err != nil {
			return fmt.Errorf("failed to set OPSG key for fact %d: %w", i, err)
		}

		// Add to graph index: GSPO (33 bytes) - Graph-Subject-Predicate-Object
		// EncodeQuadKey handles the physical reordering internally based on prefix
		gspoKey := keys.EncodeQuadKey(keys.QuadGSPOPrefix, sID, pID, oID, gID)
		if err := batch.Set(gspoKey, nil); err != nil {
			return fmt.Errorf("failed to set GSPO key for fact %d: %w", i, err)
		}

		// Update fact count (zero-cost atomic operation)
		m.numFacts.Add(1)
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	// Invalidate graphs cache since we may have added new graphs
	m.graphsCacheValid = false

	// Auto-GC: Track facts added and run GC if threshold reached
	if m.config.EnableAutoGC {
		m.factsSinceGC += uint64(len(facts))
		m.triggerAutoGC()
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

	// Delete facts in batches while iterating through GSPO index
	txn := m.db.NewTransaction(false)
	defer txn.Discard()

	prefix := keys.EncodeQuadGSPOPrefix(gID)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	type quadKeys struct {
		gspo []byte
		spog []byte
		opsg []byte
	}

	const maxDeleteBatchSize = 1000
	var keysToDelete []quadKeys
	totalDeleted := 0

	// Helper function to flush the current batch
	flushBatch := func() error {
		if len(keysToDelete) == 0 {
			return nil
		}

		deleteTxn := m.db.NewTransaction(true)
		for _, keys := range keysToDelete {
			if err := deleteTxn.Delete(keys.spog); err != nil {
				slog.Error("failed to delete SPOG key", "error", err)
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete SPOG key: %w", err)
			}
			if err := deleteTxn.Delete(keys.opsg); err != nil {
				slog.Error("failed to delete OPSG key", "error", err)
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete OPSG key: %w", err)
			}
			if err := deleteTxn.Delete(keys.gspo); err != nil {
				slog.Error("failed to delete GSPO key", "error", err)
				deleteTxn.Discard()
				return fmt.Errorf("failed to delete GSPO key: %w", err)
			}
			// Update fact count (atomic: ^uint64(0) = -1 in two's complement for uint64 subtraction)
			m.numFacts.Add(^uint64(0))
		}

		if err := deleteTxn.Commit(); err != nil {
			slog.Error("failed to commit delete transaction", "error", err)
			return fmt.Errorf("failed to commit delete batch: %w", err)
		}

		totalDeleted += len(keysToDelete)
		keysToDelete = keysToDelete[:0] // Reset slice, keep underlying array
		return nil
	}

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
		opsgKey := keys.EncodeQuadKey(keys.QuadOPSGPrefix, s, p, o, g)

		keysToDelete = append(keysToDelete, quadKeys{
			gspo: gspoKey,
			spog: spogKey,
			opsg: opsgKey,
		})

		// Flush if batch is full
		if len(keysToDelete) >= maxDeleteBatchSize {
			if err := flushBatch(); err != nil {
				it.Close()
				txn.Discard()
				return err
			}
		}
	}
	it.Close()
	txn.Discard()

	// Flush any remaining keys
	if err := flushBatch(); err != nil {
		return err
	}

	if totalDeleted == 0 {
		slog.Info("no facts found in graph", "graph", graph)
		return nil
	}

	// Invalidate graphs cache since we deleted a graph
	m.graphsCacheValid = false

	slog.Info("graph deleted successfully", "graph", graph, "factsDeleted", totalDeleted)
	return nil
}

// Query executes a Datalog query and returns results.
// Implements Multi-Atom Nested Loop Join.
func (m *MEBStore) Query(ctx context.Context, query string) ([]map[string]any, error) {
	// Parse the query into atoms
	atoms, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// Step 1: Classification
	var dataAtoms []ParsedAtom
	var constraints []ParsedAtom

	for _, atom := range atoms {
		// Currently only "triples" is supported as a data atom
		// Everything else (like "regex") is treated as a constraint
		// In the future, we could check schema or list of known predicates
		if atom.Predicate == "triples" {
			dataAtoms = append(dataAtoms, atom)
		} else {
			constraints = append(constraints, atom)
		}
	}

	if len(dataAtoms) == 0 {
		return nil, fmt.Errorf("query must contain at least one data atom (e.g. triples(...))")
	}

	// Step 2: Initialize Pipeline
	// Start with one empty binding
	bindings := []map[string]any{make(map[string]any)}

	// Step 3: Sequential Processing (Nested Loop Join)
	for _, atom := range dataAtoms {
		var nextBindings []map[string]any

		for _, binding := range bindings {
			// Substitute known variables into the DataAtom
			// triples(S, P, O) -> check if S, P, O are bound in 'binding'
			// If bound, use the value. If not, it's a variable or constant.

			// Check args count
			if len(atom.Args) != 3 {
				return nil, fmt.Errorf("triples predicate requires 3 arguments, got %d", len(atom.Args))
			}

			// Prepare Scan arguments
			scanArgs := make([]string, 3)
			vars := make(map[int]string) // Index -> Variable Name

			for i, arg := range atom.Args {
				// Datalog convention: variables start with ? or Uppercase letter
				isVar := strings.HasPrefix(arg, "?") || arg == "_" || (len(arg) > 0 && unicode.IsUpper([]rune(arg)[0]))

				if isVar {
					// Check if already bound in current binding
					if val, ok := binding[arg]; ok {
						// Bound variable -> treat as constant for this scan
						scanArgs[i] = fmt.Sprintf("%v", val)
					} else {
						// Unbound variable -> Scan wildcard
						scanArgs[i] = ""
						vars[i] = arg // Track for extraction
					}
				} else {
					// Constant -> use as is
					scanArgs[i] = strings.Trim(arg, "'\"")
				}
			}

			// Scan all graphs dynamically by discovering them from the GSPO index
			scanGraphs, err := m.getAllGraphs()
			if err != nil {
				slog.Warn("failed to discover graphs, falling back to default", "error", err)
				scanGraphs = []string{"default"}
			}

			for _, g := range scanGraphs {
				// Scan: Use the partially-bound Atom
				for fact, err := range m.Scan(scanArgs[0], scanArgs[1], scanArgs[2], g) {
					if err != nil {
						// Error scanning (e.g. not found), skip
						continue
					}

					// Expand: Create new binding
					newBinding := make(map[string]any)
					// Copy existing bindings
					for k, v := range binding {
						newBinding[k] = v
					}

					// Extract new bindings from fact
					row := []string{fact.Subject, fact.Predicate, ""}
					// Object type handling
					if s, ok := fact.Object.(string); ok {
						row[2] = s
					} else {
						row[2] = fmt.Sprintf("%v", fact.Object)
					}

					for idx, varName := range vars {
						if varName != "_" {
							newBinding[varName] = row[idx]
						}
					}
					nextBindings = append(nextBindings, newBinding)
				}
			}
		}
		// Move to next stage
		bindings = nextBindings
		if len(bindings) == 0 {
			break // Short-circuit if no results
		}
	}

	// Step 4: Post-Filter (Constraints)
	var finalResults []map[string]any
	for _, row := range bindings {
		keep := true
	ConstraintLoop:
		for _, c := range constraints {
			switch c.Predicate {
			case "regex":
				// Format: regex(Var, "pattern")
				if len(c.Args) != 2 {
					return nil, fmt.Errorf("regex constraint requires 2 arguments")
				}
				varName := c.Args[0]
				pattern := strings.Trim(c.Args[1], "\"'")

				val, ok := row[varName]
				if !ok {
					// If the variable used in regex is NOT bound, it fails?
					// Or do we ignore? Constraint implication: strict filter.
					keep = false
					break ConstraintLoop
				}
				valStr := fmt.Sprintf("%v", val)

				matched, err := regexp.MatchString(pattern, valStr)
				if err != nil {
					return nil, fmt.Errorf("invalid regex pattern '%s': %w", pattern, err)
				}
				if !matched {
					keep = false
					break ConstraintLoop
				}
			case "neq":
				// Format: neq(A, B) or A != B
				if len(c.Args) != 2 {
					return nil, fmt.Errorf("neq constraint requires 2 arguments")
				}
				lhsArg := c.Args[0]
				rhsArg := c.Args[1]

				// Helper to resolve value: either variable look up or constant
				resolveVal := func(arg string) string {
					// Check if variable (Uppercase/?)
					isVar := strings.HasPrefix(arg, "?") || arg == "_" || (len(arg) > 0 && unicode.IsUpper([]rune(arg)[0]))
					if isVar {
						if val, ok := row[arg]; ok {
							return fmt.Sprintf("%v", val)
						}
						return "" // Unbound variable?
					}
					// Constant
					return strings.Trim(arg, "\"'")
				}

				valA := resolveVal(lhsArg)
				valB := resolveVal(rhsArg)

				if valA == valB {
					keep = false
					break ConstraintLoop
				}
			default:
				return nil, fmt.Errorf("unknown constraint predicate: %s", c.Predicate)
			}
		}

		if keep {
			finalResults = append(finalResults, row)
		}
	}

	return finalResults, nil
}

// getAllGraphs discovers all unique graphs in the store.
// Returns cached results if available, otherwise scans and caches.
// This is an expensive operation on first call; subsequent calls use the cache.
func (m *MEBStore) getAllGraphs() ([]string, error) {
	// Return cached result if valid
	if m.graphsCacheValid {
		return m.graphsCache, nil
	}

	graphSet := make(map[uint64]struct{})

	txn := m.db.NewTransaction(false)
	defer txn.Discard()

	prefix := []byte{keys.QuadGSPOPrefix}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		if len(key) != keys.QuadKeySize {
			continue
		}

		// Decode to get the graph ID
		_, _, _, gID := keys.DecodeQuadKey(key)
		if gID != 0 {
			graphSet[gID] = struct{}{}
		}
	}

	if len(graphSet) == 0 {
		m.graphsCache = []string{"default"}
		m.graphsCacheValid = true
		return m.graphsCache, nil
	}

	// Convert IDs to strings
	graphs := make([]string, 0, len(graphSet))
	for gID := range graphSet {
		gName, err := m.dict.GetString(gID)
		if err != nil {
			// Skip graphs that can't be resolved
			slog.Debug("failed to resolve graph ID", "graphID", gID, "error", err)
			continue
		}
		graphs = append(graphs, gName)
	}

	// Sort for consistent ordering
	sort.Strings(graphs)

	// Cache the result
	m.graphsCache = graphs
	m.graphsCacheValid = true

	return graphs, nil
}
