package query

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

// LFTJQuery represents a multi-way join query for LFTJ execution
type LFTJQuery struct {
	// Relations is a list of relation patterns to join
	// Each pattern defines: prefix (index type) + bound values
	Relations []RelationPattern

	// BoundVars maps variable positions to their bound values
	BoundVars map[string]uint64

	// ResultVars defines the order of variables in the result
	ResultVars []string

	// ColumnOrders stores the column order for each relation (used for result mapping)
	ColumnOrders [][]int
}

// RelationPattern defines a single relation to join
type RelationPattern struct {
	// Prefix is the index type (QuadSPOGPrefix, QuadOPSGPrefix, etc.)
	Prefix byte

	// BoundPositions maps attribute positions to bound values
	// Position: 0=Subject, 1=Predicate, 2=Object, 3=Graph
	BoundPositions map[int]uint64

	// VariablePositions maps attribute positions to variable names
	// Unbound positions that need to be joined
	VariablePositions map[int]string
}

// LFTJEngine executes queries using Leapfrog Triejoin
type LFTJEngine struct {
	db *badger.DB
}

// NewLFTJEngine creates a new LFTJ query engine
func NewLFTJEngine(db *badger.DB) *LFTJEngine {
	return &LFTJEngine{db: db}
}

// Execute runs a multi-way join query using LFTJ
// Returns an iterator over joined tuples
func (e *LFTJEngine) Execute(ctx context.Context, query LFTJQuery) iter.Seq2[map[string]uint64, error] {
	return func(yield func(map[string]uint64, error) bool) {
		// Create a read-only transaction
		txn := e.db.NewTransaction(false)
		defer txn.Discard()

		// Build iterators for each relation
		iterators := make([]*TrieIterator, 0, len(query.Relations))
		columnOrders := make([][]int, len(query.Relations))
		for i, rel := range query.Relations {
			// Build prefix for this relation
			prefix := e.buildPrefix(rel)

			// Determine column order based on prefix type
			columnOrder := e.getColumnOrder(rel.Prefix)
			columnOrders[i] = columnOrder

			// Create iterator
			it := NewTrieIterator(txn, prefix, columnOrder)
			if err := it.Open(); err != nil {
				yield(nil, fmt.Errorf("failed to open iterator for relation %d: %w", i, err))
				return
			}
			iterators = append(iterators, it)
		}

		// Close all iterators when done
		defer func() {
			for _, it := range iterators {
				it.Close()
			}
		}()

		// Execute leapfrog join
		for tuple, err := range LeapfrogJoin(iterators) {
			if err != nil {
				yield(nil, err)
				return
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			// Build result map from tuple using column orders for proper mapping
			result := e.buildResult(query, tuple, columnOrders)
			if !yield(result, nil) {
				return
			}
		}
	}
}

// buildPrefix builds the seek prefix for a relation pattern
func (e *LFTJEngine) buildPrefix(rel RelationPattern) []byte {
	// Start with the prefix byte
	prefix := []byte{rel.Prefix}

	// Add bound values in order
	// For quad keys: Subject(0), Predicate(1), Object(2), Graph(3)
	for pos := 0; pos < 4; pos++ {
		if val, isBound := rel.BoundPositions[pos]; isBound {
			// Add 8-byte value
			valBytes := make([]byte, 8)
			valBytes[0] = byte(val >> 56)
			valBytes[1] = byte(val >> 48)
			valBytes[2] = byte(val >> 40)
			valBytes[3] = byte(val >> 32)
			valBytes[4] = byte(val >> 24)
			valBytes[5] = byte(val >> 16)
			valBytes[6] = byte(val >> 8)
			valBytes[7] = byte(val)
			prefix = append(prefix, valBytes...)
		} else {
			// Stop at first unbound position for prefix
			break
		}
	}

	return prefix
}

// getColumnOrder returns the column order for a given prefix type
func (e *LFTJEngine) getColumnOrder(prefix byte) []int {
	switch prefix {
	case keys.QuadSPOGPrefix:
		// SPOG: Subject(0), Predicate(1), Object(2), Graph(3)
		return []int{0, 1, 2, 3}
	case keys.QuadOPSGPrefix:
		// OPSG: Object(2), Predicate(1), Subject(0), Graph(3)
		return []int{2, 1, 0, 3}
	case keys.QuadGSPOPrefix:
		// GSPO: Graph(3), Subject(0), Predicate(1), Object(2)
		return []int{3, 0, 1, 2}
	default:
		// Default to SPOG
		return []int{0, 1, 2, 3}
	}
}

// buildResult creates a result map from a joined tuple.
// Uses columnOrders to map tuple positions to logical positions (Subject, Predicate, Object, Graph),
// then uses VariablePositions to assign values to result variables.
func (e *LFTJEngine) buildResult(query LFTJQuery, tuple []uint64, columnOrders [][]int) map[string]uint64 {
	result := make(map[string]uint64)

	// Trace: Log tuple and column orders in development mode
	slog.Debug("buildResult: input tuple", "tuple", tuple, "numRelations", len(query.Relations))

	// Add bound variables
	for varName, val := range query.BoundVars {
		result[varName] = val
	}

	// Map tuple values to result variables using column orders
	// columnOrders[i][j] = logical position (0=Subject, 1=Predicate, 2=Object, 3=Graph) at tuple position j
	tuplePos := 0
	for relIdx, rel := range query.Relations {
		columnOrder := columnOrders[relIdx]

		slog.Debug("buildResult: processing relation", "relIdx", relIdx, "columnOrder", columnOrder, "varPositions", rel.VariablePositions)

		// Process each position in the tuple for this relation
		for _, logicalPos := range columnOrder {
			if tuplePos >= len(tuple) {
				break
			}

			// Check if this logical position has a variable
			if varName, isVar := rel.VariablePositions[logicalPos]; isVar {
				// Skip underscore variables
				if varName != "_" && !strings.HasPrefix(varName, "_") {
					result[varName] = tuple[tuplePos]
					slog.Debug("buildResult: assigned variable", "varName", varName, "value", tuple[tuplePos], "logicalPos", logicalPos)
				}
			}
			tuplePos++
		}
	}

	slog.Debug("buildResult: result", "result", result)
	return result
}

// ExecuteTriangleCount performs a triangle counting query using LFTJ
// Query: Find all (A, B, C) where A->B, B->C, C->A
func (e *LFTJEngine) ExecuteTriangleCount(ctx context.Context) iter.Seq2[[][]uint64, error] {
	return func(yield func([][]uint64, error) bool) {
		query := LFTJQuery{
			Relations: []RelationPattern{
				{
					Prefix:            keys.QuadSPOGPrefix,
					BoundPositions:    map[int]uint64{},
					VariablePositions: map[int]string{0: "A", 1: "p1", 2: "B"},
				},
				{
					Prefix:            keys.QuadSPOGPrefix,
					BoundPositions:    map[int]uint64{},
					VariablePositions: map[int]string{0: "B", 1: "p2", 2: "C"},
				},
				{
					Prefix:            keys.QuadSPOGPrefix,
					BoundPositions:    map[int]uint64{},
					VariablePositions: map[int]string{0: "C", 1: "p3", 2: "A"},
				},
			},
			ResultVars: []string{"A", "B", "C"},
		}

		// Execute and collect results
		results := make([][]uint64, 0)
		for result, err := range e.Execute(ctx, query) {
			if err != nil {
				yield(nil, err)
				return
			}

			triangle := make([]uint64, 3)
			if a, ok := result["A"]; ok {
				triangle[0] = a
			}
			if b, ok := result["B"]; ok {
				triangle[1] = b
			}
			if c, ok := result["C"]; ok {
				triangle[2] = c
			}
			results = append(results, triangle)
		}

		if !yield(results, nil) {
			return
		}
	}
}

// LFTJStats tracks statistics for LFTJ query execution
type LFTJStats struct {
	NumJoins      int
	NumResults    int
	NumSeeks      int
	NumNexts      int
	ExecutionTime int64 // nanoseconds
}

// Stats returns execution statistics
func (e *LFTJEngine) Stats() LFTJStats {
	// This would track actual stats during execution
	return LFTJStats{}
}
