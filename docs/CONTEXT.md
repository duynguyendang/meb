---
context_type: kernel_source_dump
project: meb_v2
language: go
last_updated: 2026-03-03T00:00:00Z
scan_mode: logic_focused
---

# MEB v2 (Mangle Extension for Badger) - Live Architecture Snapshot

This document serves as the canonical, authoritative "Live Architecture" snapshot of the MEB v2 codebase. It contains the exact interfaces, structs, and logic that drive the Knowledge Graph Store with integrated Vector Similarity Search.

## 1. THE COMPLETE FILE MAP

```text
.
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
│   ├── lftj.go                # Leapfrog Triejoin
│   └── optimizer.go           # Cost-based optimizer
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
    db           *badger.DB
    dictDB       *badger.DB       // Separate DB for dictionary
    dict         dict.Dictionary  // String <-> ID encoding
    txPool       *TxPool          // Transaction pooling
    predicates   map[ast.PredicateSym]*predicates.PredicateTable
    config       *store.Config
    numFacts     atomic.Uint64    // In-memory fact counter
    vectors      *vector.VectorRegistry  // MRL vector search
    breaker      *circuit.Breaker        // Query timeout protection
    graphsCache      []string            // Cached graph list
    graphsCacheValid bool                 // Cache validity flag
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

// Key format: [prefix(1) | subject(8) | predicate(8) | object(8) | graph(8)]
// Uses BigEndian for lexicographic ordering
```

---

## 4. CORE OPERATIONS

### 4.1 Adding Facts (Dual-Index Write)

```go
package meb

// AddFactBatch inserts multiple facts with dual-indexing:
// - SPOG: Forward traversal by subject
// - OPSG: Reverse lookup by object
// - GSPO: Graph-scoped operations
func (m *MEBStore) AddFactBatch(facts []Fact) error {
    // 1. Batch encode all unique strings to IDs
    ids, err := m.dict.GetIDs(uniqueStrings)
    
    // 2. Build BadgerDB batch with all three index keys
    for i, fact := range facts {
        spogKey := keys.EncodeQuadKey(keys.QuadSPOGPrefix, sID, pID, oID, gID)
        opsgKey := keys.EncodeQuadKey(keys.QuadOPSGPrefix, oID, pID, sID, gID)
        gspoKey := keys.EncodeQuadKey(keys.QuadGSPOPrefix, sID, pID, oID, gID)
        
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

### 5.1 Datalog Query (Nested Loop Join)

```go
package meb

// Query executes Datalog queries with multi-atom joins:
// 1. Parse query into atoms
// 2. Classify as data atoms vs constraints
// 3. Sequential nested loop join
// 4. Apply post-filter constraints (regex, neq)
func (m *MEBStore) Query(ctx context.Context, query string) ([]map[string]any, error)
```

### 5.2 LFTJ (Leapfrog Triejoin)

```go
package query

// LFTJEngine provides worst-case optimal multi-way joins:
// - O(N × |output|) vs O(∏|Rᵢ|) for nested loops
// - No intermediate materialization
// - Trie-based prefix matching
type LFTJEngine struct {
    db *badger.DB
}

func (e *LFTJEngine) Execute(ctx context.Context, q LFTJQuery) iter.Seq2[map[string]uint64, error]
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

## 9. KEY DESIGN DECISIONS

1. **Dual BadgerDB**: Separate DBs for facts (async) and dictionary (sync)
2. **Sharded Dictionary**: 32+ shards with per-shard LRU cache
3. **Atomic Fact Count**: Lock-free counter in RAM, persisted on shutdown
4. **Zero-Copy Iterator**: Go 1.23 iter.Seq2 for streaming without allocation
5. **Circuit Breaker**: 2-second hard limit on all queries
6. **Graph Caching**: Avoids rescanning GSPO index on every query

---

## 10. DEPENDENCIES

- **dgraph-io/badger/v4**: Key-value storage
- **google/mangle**: Datalog reasoning engine
- **klauspost/compress**: S2 compression
- **hashicorp/golang-lru**: Dictionary caching
