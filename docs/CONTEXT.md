---
context_type: kernel_source_dump
project: meb_v2
language: go
last_updated: 2026-03-13T00:00:00Z
scan_mode: logic_focused
---

# MEB v2 (Mangle Extension for Badger) - Live Architecture Snapshot

This document serves as the canonical, authoritative "Live Architecture" snapshot of the MEB v2 codebase. It contains the exact interfaces, structs, and logic that drive the Knowledge Graph Store with integrated Vector Similarity Search.

## 1. THE COMPLETE FILE MAP

```
meb/
├── adapter/                     # Mangle Datalog adapter
├── circuit/                    # Circuit breaker for query timeouts
├── cluster/                    # Clustering utilities
├── clustering/                 # Leiden algorithm & hybrid clustering
├── cmd/                        # CLI tools (stress testing)
├── content.go                  # Content storage with S2 compression
├── dict/                       # String interning (sharded encoder)
│   ├── encoder.go             # Single-threaded dictionary
│   ├── sharded.go             # Concurrent sharded dictionary
│   └── interface.go           # Dictionary interface
├── docs/                       # Architecture documentation
│   ├── CSD.md                  # Conceptual Solution Design
│   ├── HLD.md                  # High Level Design
│   └── CONTEXT.md              # This file
├── errors.go                   # Custom error types
├── fact.go                     # Fact (Quad) struct
├── fact_store.go               # factstore.FactStore implementation
├── generics.go                 # Generic helpers (Collect, Filter, Value)
├── keys/                       # Key encoding (SPOG, OPSG, GSPO)
│   └── encoding.go             # 33-byte quad key encoding
├── knowledge_store.go          # Core KnowledgeStore implementation
├── options.go                  # Configuration options
├── predicates/                 # Predicate tables
├── query/                      # LFTJ engine & CBO optimizer
│   ├── lftj.go                # Leapfrog Triejoin (with branch isolation)
│   ├── engine.go              # LFTJ execution engine
│   ├── optimizer.go           # Cost-based optimizer (with index selection)
│   ├── executor.go            # Query executor
│   └── executor_test.go       # Query tests
├── query_builder.go            # Neuro-symbolic query builder
├── query_result.go             # Query result types
├── scan.go                     # Quad scan operations
├── store/                      # BadgerDB configuration
│   └── badger.go              # Config & BadgerDB setup
├── store_manager.go            # Store lifecycle management
├── tx.go                       # Transaction helpers
├── utils/                      # Utility functions
└── vector/                     # Vector storage & search
    ├── registry.go             # Vector registry
    ├── storage.go              # MRL + SQ8 persistence
    ├── quantization.go         # INT8 quantization
    ├── pq.go                   # Product Quantization & Hybrid search
    ├── search.go               # Similarity search
    └── simd_int8.go            # SIMD-accelerated dot product
```

---

## 2. CORE DATA STRUCTURES

### 2.1 Fact (The Universal Quad)

```go
package meb

// Fact represents a single Quad (Subject-Predicate-Object-Graph) in the knowledge base.
// This format supports multi-tenancy and RAG contexts by including a Graph identifier.
type Fact struct {
    Subject   string // The subject entity
    Predicate string // The predicate/relation
    Object    any    // The object value (can be string, int, float64, bool, etc.)
    Graph     string // The graph/context identifier. Defaults to "default" if empty.
}
```

### 2.2 MEBStore (The Core Engine)

```go
package meb

import (
    "sync/atomic"
    "github.com/dgraph-io/badger/v4"
    "github.com/duynguyendang/meb/dict"
    "github.com/duynguyendang/meb/vector"
    "github.com/duynguyendang/meb/circuit"
)

// MEBStore implements both factstore.FactStore and store.KnowledgeStore interfaces.
// It uses BadgerDB for persistent storage and dictionary encoding for efficient operations.
type MEBStore struct {
    db               *badger.DB
    dictDB           *badger.DB       // Separate DB for dictionary
    dict             dict.Dictionary  // String <-> ID encoding
    txPool           *TxPool          // Transaction pooling
    predicates       map[ast.PredicateSym]*predicates.PredicateTable
    config           *store.Config
    numFacts         atomic.Uint64    // In-memory fact counter
    vectors          *vector.VectorRegistry  // MRL vector search
    breaker          *circuit.Breaker        // Query timeout protection
    graphsCache      []string                 // Cached graph list
    graphsCacheValid bool                     // Cache validity flag
    factsSinceGC     uint64                   // GC tracking
}
```

### 2.3 Configuration

```go
package store

// Config holds the configuration for BadgerDB.
type Config struct {
    DataDir        string  // Main data directory
    DictDir        string  // Dictionary data directory
    InMemory       bool    // In-memory mode
    BlockCacheSize int64   // Block cache size (default: 8GB)
    IndexCacheSize int64   // Index cache size (default: 2GB)
    LRUCacheSize   int     // Dictionary LRU cache (default: 100k)
    Compression    bool    // ZSTD compression (default: true)
    SyncWrites     bool    // Synchronous writes
    NumDictShards  int     // Dictionary shards (0 = single-threaded)
    MemTableSize   int64   // Memtable size (16MB/64MB based on profile)
    NumMemtables   int     // Number of memtables
    Profile        string  // "Ingest-Heavy", "Safe-Serving", "ReadOnly"
    ReadOnly       bool    // Read-only mode
    EnableAutoGC   bool    // Enable automatic GC
}
```

---

## 3. KEY ENCODING (keys/encoding.go)

MEB uses 33-byte quad keys for efficient bidirectional graph traversal:

```go
package keys

const (
    QuadSPOGPrefix byte = 0x20 // Subject-Predicate-Object-Graph
    QuadOPSGPrefix byte = 0x21 // Object-Predicate-Subject-Graph  
    QuadGSPOPrefix byte = 0x22 // Graph-Subject-Predicate-Object
    QuadKeySize    int   = 33  // 1 + 4*8 bytes
)

// Key formats:
// SPOG: [prefix(1) | subject(8) | predicate(8) | object(8) | graph(8)]
// OPSG: [prefix(1) | object(8) | predicate(8) | subject(8) | graph(8)]
// GSPO: [prefix(1) | graph(8) | subject(8) | predicate(8) | object(8)]
// Uses BigEndian for lexicographic ordering
```

---

## 4. CORE OPERATIONS

### 4.1 Adding Facts (Triple-Index Write)

```go
package meb

// AddFactBatch inserts multiple facts with triple-indexing:
// - SPOG: Forward traversal by subject
// - OPSG: Reverse lookup by object
// - GSPO: Graph-scoped operations (Graph first for efficient filtering)
func (m *MEBStore) AddFactBatch(facts []Fact) error {
    // 1. Batch encode all unique strings to IDs (single call, minimal locking)
    ids, err := m.dict.GetIDs(uniqueStrings)
    
    // 2. Build BadgerDB batch with all three index keys
    for i, fact := range facts {
        // SPOG: Subject-Predicate-Object-Graph (forward lookups)
        spogKey := keys.EncodeQuadKey(keys.QuadSPOGPrefix, sID, pID, oID, gID)
        
        // OPSG: Object-Predicate-Subject-Graph (reverse lookups)
        opsgKey := keys.EncodeQuadKey(keys.QuadOPSGPrefix, oID, pID, sID, gID)
        
        // GSPO: Graph-Subject-Predicate-Object (graph-scoped queries)
        gspoKey := keys.EncodeQuadKey(keys.QuadGSPOPrefix, gID, sID, pID, oID)
        
        batch.Set(spogKey, nil)
        batch.Set(opsgKey, nil)
        batch.Set(gspoKey, nil)
        
        m.numFacts.Add(1)
    }
    
    return batch.Flush()
}
```

### 4.2 Scanning Facts (Intelligent Index Selection)

```go
package meb

// Scan returns an iterator over facts matching the pattern.
// Automatically selects optimal index based on bound arguments:
//
// - Graph bound only     -> GSPO index
// - Subject bound       -> SPOG index  
// - Object bound        -> OPSG index
// - Predicate bound     -> SPOG index (fallback)
// - Nothing bound       -> SPOG index (full scan)
func (m *MEBStore) Scan(s, p, o, g string) iter.Seq2[Fact, error]
```

### 4.3 Vector Search (Neuro-Symbolic Queries)

```go
package meb

// Builder provides fluent API for hybrid vector + graph queries
type Builder struct {
    vectorQuery         []float32
    threshold           float32
    filters             []GraphFilter
    graph               string
    limit               int
    candidateMultiplier int
}

// Execute runs Neuro-First strategy:
// 1. Vector search to get candidates
// 2. Filter through graph constraints
// 3. Return hydrated results
func (b *Builder) Execute() ([]Result, error)
```

---

## 5. QUERY PIPELINE

### 5.1 LFTJ (Leapfrog Triejoin) - Core Engine

```go
package query

// TrieIterator provides trie-structured access to a relation stored in BadgerDB.
// Key features:
// - Branch isolation via depthPrefixes
// - Backtracking support via Up()/Down()
// - AdvancedByChild tracking to prevent skipping siblings
type TrieIterator struct {
    txn            *badger.Txn
    it             *badger.Iterator
    prefix         []byte        // Current position prefix
    depth          int           // Current trie depth (0=first ID, 1=second ID, etc.)
    columnOrder    []int         // Attribute positions: [0,1,2,3] for SPOG
    exhausted      bool          // Iterator exhausted flag
    depthPrefixes [][]byte      // Required prefix at each level for branch isolation
    advancedByChild bool         // Prevent skipping siblings after child-level ops
}

// Key methods:
// - Open()     : Initialize iterator at root
// - Seek(target): Seek to key, stay within branch
// - Next()     : Move to next sibling, stay within branch
// - Up()       : Move up, reset exhausted for backtracking
// - Down()     : Move down, track depth prefix
// - Key()      : Get current key at depth
// - AtEnd()    : Check if exhausted
```

### 5.2 LFTJ Query Execution

```go
package query

// LFTJQuery represents a multi-way join query for LFTJ execution
type LFTJQuery struct {
    Relations     []RelationPattern  // Relation patterns to join
    BoundVars     map[string]uint64 // Bound variable values
    ResultVars    []string          // Result variable order
    ColumnOrders  [][]int           // Column order per relation for result mapping
}

// RelationPattern defines a single relation to join
type RelationPattern struct {
    Prefix            byte           // Index type (SPOG/OPSG/GSPO)
    BoundPositions   map[int]uint64 // Bound positions: 0=S, 1=P, 2=O, 3=G
    VariablePositions map[int]string // Variable positions for joining
}

// LFTJEngine provides worst-case optimal multi-way joins:
// - O(N × |output|) vs O(∏|Rᵢ|) for nested loops
// - No intermediate materialization
// - Trie-based prefix matching
type LFTJEngine struct {
    db *badger.DB
}

func (e *LFTJEngine) Execute(ctx context.Context, q LFTJQuery) iter.Seq2[map[string]uint64, error)
```

### 5.3 Query Optimizer (Index Selection)

```go
package query

// CompileToLFTJ compiles an optimized QueryPlan into an executable LFTJQuery.
// Features:
// - Constant resolution at compile time via Dictionary
// - Index selection based on bound positions:
//   * Subject bound  -> SPOG index (efficient)
//   * Object bound   -> OPSG index (efficient) 
//   * Both bound     -> SPOG index (preferred)
//   * Neither bound -> SPOG index (default)
// - BoundPositions remapping for OPSG key layout
func (qo *QueryOptimizer) CompileToLFTJ(plan *QueryPlan, dictionary dict.Dictionary) (LFTJQuery, error)
```

### 5.4 Result Building

```go
package query

// buildResult creates a result map from a joined tuple.
// Uses columnOrders to map tuple positions to logical positions:
// columnOrders[i][j] = logical position (0=S, 1=P, 2=O, 3=G) at tuple position j
// Then uses VariablePositions to assign values to result variables.
func (e *LFTJEngine) buildResult(query LFTJQuery, tuple []uint64, columnOrders [][]int) map[string]uint64
```

### 5.5 Datalog Query (Nested Loop Join)

```go
package meb

// Query executes Datalog queries with multi-atom joins:
// 1. Parse query into atoms
// 2. Classify as data atoms vs constraints
// 3. Sequential nested loop join
// 4. Apply post-filter constraints (regex, neq)
func (m *MEBStore) Query(ctx context.Context, query string) ([]map[string]any, error)
```

---

## 6. VECTOR OPERATIONS

### 6.1 Vector Registry

```go
package vector

const (
    FullDim int = 3072  // OpenAI embedding dimension
    MRLDim  int = 64    // Matryoshka Reduced dimension
)

// VectorRegistry manages vector storage and search
type VectorRegistry struct {
    db         *badger.DB
    data       []int8        // SQ8 quantized vectors (64 bytes each)
    pqData     []byte        // PQ encoded vectors (8 bytes each)
    pqCodebook *PQCodebook   // Trained PQ codebook
    idMap      map[uint64]uint32  // ID -> index
    revMap     []uint64       // index -> ID
}
```

### 6.2 Processing Pipeline

```
Input: 3072-d float32 (OpenAI)
  ↓
MRL: 3072-d → 64-d (Matryoshka Representation Learning)
  ↓
L2 Normalize: Unit vectors
  ↓
SQ8 / PQ: float32 → int8 (SQ8) & byte (PQ)
  ↓
Search: Hierarchical Search (Coarse SIMD dot product MRL → Fine PQ approximate distance)
```

---

## 7. CLUSTERING

### 7.1 Community Detection (Leiden Algorithm)

```go
package clustering

// CommunityDetector implements Leiden algorithm for graph partitioning
type CommunityDetector struct {
    db *badger.DB
}

// Detect runs Leiden algorithm to find communities
func (d *CommunityDetector) Detect(graphID string) (*CommunityHierarchy, error)

// HybridClustering combines static Leiden + dynamic K-means
type HybridClustering struct {
    db *badger.DB
}
```

---

## 8. CONFIGURATION PROFILES

### 8.1 Ingest-Heavy (Default)

- ValueLogFileSize: 1GB
- MemTableSize: 64MB
- NumMemtables: 3
- NumCompactors: 4
- NumVersionsToKeep: 0 (keep all)

### 8.2 Safe-Serving (Cloud Run, Low RAM)

- ValueLogFileSize: 64MB
- MemTableSize: 16MB
- NumMemtables: 2
- NumCompactors: 2
- NumVersionsToKeep: 1

### 8.3 ReadOnly

- ValueLogFileSize: 16MB
- MemTableSize: 16MB
- ReadOnly: true

---

## 9. LATEST ENGINE FIXES (2026-03-13)

### 9.1 LFTJ Branch Isolation

**Problem**: TrieIterator could wander into adjacent branches during join execution.

**Solution**: Added `depthPrefixes` tracking:
- `depthPrefixes[d]` stores the required prefix at depth `d`
- `Next()` and `Seek()` validate against depthPrefixes before advancing
- `advancedByChild` flag prevents skipping siblings after child-level operations

```go
// Down() tracks depth prefix
func (ti *TrieIterator) Down() {
    key := ti.it.Item().Key()
    prefixLen := len(ti.prefix) + (ti.depth+1)*8
    ti.depthPrefixes[ti.depth+1] = key[:prefixLen]
    ti.depth++
    ti.advancedByChild = true
}

// Next() validates against depth prefix
func (ti *TrieIterator) Next() bool {
    if ti.advancedByChild && ti.depthPrefixes[ti.depth] != nil {
        // Check still within branch
        if !validPrefix(key, ti.depthPrefixes[ti.depth]) {
            ti.exhausted = true
            return false
        }
    }
    ti.it.Next()
    // ...
}
```

### 9.2 Backtracking Fix

**Problem**: `Up()` didn't reset exhausted flag, preventing continued exploration at higher levels.

**Solution**: Reset both `exhausted` and `advancedByChild` in `Up()`:

```go
func (ti *TrieIterator) Up() {
    if ti.depth > 0 {
        ti.depth--
        ti.exhausted = false      // Allow continued exploration
        ti.advancedByChild = false
    }
}
```

### 9.3 Iterator Pointer Bug

**Problem**: `leapfrogRecursive` used `info.iter` which was a copy, not a pointer.

**Solution**: Use `iterators[info.index]` for all iterator operations:

```go
// Before (broken): info.iter.Down()
// After (fixed):
for _, info := range infos {
    iterators[info.index].Down()   // Use pointer from original slice
    iterators[info.index].Up()
    iterators[info.index].Next()
    iterators[info.index].Seek(maxKey)
}
```

### 9.4 OPSG Index Selection

**Problem**: When selecting OPSG index for object-bound queries, BoundPositions weren't remapped.

**Solution**: Remap logical positions to OPSG key positions:

```go
if objectBound && !subjectBound {
    rel.Prefix = keys.QuadOPSGPrefix
    // Remap: logical pos 2 (Object) -> key pos 0
    newBound := make(map[int]uint64)
    if boundObj, ok := rel.BoundPositions[2]; ok {
        newBound[0] = boundObj // Object at key position 0
    }
    if boundPred, ok := rel.BoundPositions[1]; ok {
        newBound[1] = boundPred // Predicate at key position 1
    }
    rel.BoundPositions = newBound
}
```

### 9.5 Result Variable Mapping

**Problem**: Results were incorrectly mapped when using different index types (SPOG vs OPSG).

**Solution**: Use columnOrders to map tuple positions to logical positions:

```go
func (e *LFTJEngine) buildResult(query LFTJQuery, tuple []uint64, columnOrders [][]int) map[string]uint64 {
    tuplePos := 0
    for relIdx, rel := range query.Relations {
        columnOrder := columnOrders[relIdx]  // e.g., [2,1,0,3] for OPSG
        
        for _, logicalPos := range columnOrder {  // logicalPos = S/P/O/G
            if varName, isVar := rel.VariablePositions[logicalPos]; isVar {
                result[varName] = tuple[tuplePos]
            }
            tuplePos++
        }
    }
}
```

### 9.6 GSPO Index Encoding Fix

**Problem**: GSPO index was encoded incorrectly as `(s,p,o,g)` instead of `(g,s,p,o)`.

**Solution**: Fixed encoding order in AddFactBatch:

```go
// Before (broken): gspoKey := keys.EncodeQuadKey(keys.QuadGSPOPrefix, sID, pID, oID, gID)
// After (fixed):
gspoKey := keys.EncodeQuadKey(keys.QuadGSPOPrefix, gID, sID, pID, oID)
```

---

## 10. KEY DESIGN DECISIONS

1. **Dual BadgerDB**: Separate DBs for facts (async) and dictionary (sync)
2. **Sharded Dictionary**: 32+ shards with per-shard LRU cache
3. **Atomic Fact Count**: Lock-free counter in RAM, persisted on shutdown
4. **Zero-Copy Iterator**: Go 1.23 iter.Seq2 for streaming without allocation
5. **Circuit Breaker**: 2-second hard limit on all queries
6. **Graph Caching**: Avoids rescanning GSPO index on every query
7. **Triple Index**: SPOG + OPSG + GSPO for efficient forward/reverse/graph-scoped queries
8. **Branch Isolation**: depthPrefixes prevent trie wander bugs in LFTJ

---

## 11. DEPENDENCIES

- **dgraph-io/badger/v4**: Key-value storage
- **google/mangle**: Datalog reasoning engine
- **klauspost/compress**: S2 compression
- **hashicorp/golang-lru**: Dictionary caching

---

## 12. TESTING

```bash
# Run all tests
go test ./... -v

# Run LFTJ-specific tests
go test ./query/... -v -run LFTJ

# Run knowledge store tests
go test -v -run TestKnowledgeStore

# Benchmark LFTJ
go test ./query/... -bench=BenchmarkLFTJ -benchmem
```
