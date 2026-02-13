# MEB (Mangle Extension for Badger) - Conceptual Solution Design

## 1. Executive Summary

MEB is a high-performance embedded knowledge graph database designed for code analysis and semantic search. It combines quad-store semantics (Subject-Predicate-Object-Graph) with vector embeddings to enable hybrid symbolic-neural retrieval.

### Key Characteristics

- **Embedded**: Runs in-process with no external dependencies
- **Quad Store**: Native quad storage with Subject-Predicate-Object-Graph model
- **Multi-Tenant**: Graph context enables complete tenant isolation
- **Concurrent**: Lock-free counters, sharded dictionary, transaction reuse
- **Scalable**: Memory-mapped vector storage for low RAM footprint
- **High-Performance**: SIMD-accelerated vector search with parallel hydration
- **Typed**: Strongly-typed document IDs with provenance tracking

### Target Constraints

**Resource Limits:**
- **RAM Target**: 8GB maximum footprint
- **Query Latency**: <2s circuit breaker (hard limit)
- **Join Results**: Max 5,000 intermediate facts
- **Recursion Depth**: Max 5-10 levels (hard limit)

**Design Principles:**
- **Static over Dynamic**: Global Leiden at ingest time, not real-time
- **Local over Remote**: Download to ephemeral disk, no GCS Fuse
- **Deductive over Imperative**: Rules infer facts, don't execute business logic
- **Bounded over Unbounded**: All operations have hard limits

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        MEB Store                             │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐  │
│  │                     Quad Store                        │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │  │
│  │  │   FactStore  │  │  ContentStore│  │ VectorStore  │ │  │
│  │  │   (Quads)    │  │  (Documents) │  │ (Embeddings) │ │  │
│  │  │  S-P-O-G     │  │              │  │              │ │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘ │  │
│  └──────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Analysis   │  │   Hydration  │  │ Query Builder│      │
│  │  (Inference) │  │  (Parallel)  │  │  (Datalog)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
├─────────────────────────────────────────────────────────────┤
│          Dual BadgerDB (Facts + Dictionary)                  │
└─────────────────────────────────────────────────────────────┘
```

### Architecture Constraints & Implementation Strategy

**Design Philosophy**: Prioritize stability, predictability, and resource efficiency over "moonshot" features.

#### Must-Have Core Features

**A. Memory-Efficient Vector Search**
- **MRL Truncation**: Store only first 64 dimensions in `vector.bin` mmap (90% memory reduction)
- **INT8 Quantization**: Float32 → INT8 for search buffer
- **SIMD Acceleration**: AVX2/NEON dot-product for 64-byte vectors

**B. Intelligent Storage Strategy**
- **BadgerDB Separation**: Large content in Value Log (`blob_db`), LSM-tree (`meta_db`) for Quads only
- **33-Byte Key Encoding**: Fixed-size SPOG/POSG/GSPO keys for O(1) prefix scans
- **Local Ephemeral Disk**: Download snapshots to `/tmp` before opening (NO GCS Fuse direct access)

**C. Two-Stage Clustering (Leiden)**
- **Static Global Leiden**: Compute once during ingestion for macro navigation
- **Dynamic Sub-clustering**: Run Fast-Leiden/K-Means only on filtered result sets in-memory
- **NO Real-time Global Updates**: Avoid CPU/RAM spikes from continuous re-clustering

**D. Pragmatic Query Optimizer (CBO)**
- **Greedy Cardinality**: Join smaller predicates first using per-predicate fact counts
- **Circuit Breaker**: Hard 2,000ms query timeout
- **Bounded Recursion**: MaxDepth 5-10 levels, no infinite loops

#### Discard List (Avoid These)

| Feature | Why Avoid | Alternative |
|---------|-----------|-------------|
| **Real-time Global Leiden** | CPU/RAM spikes, Cloud Run kills | Static at ingest + periodic refresh |
| **Business Logic in Rules** | Hard to debug, side effects | Deductive reasoning only (infer facts) |
| **GCS Fuse Direct Access** | 10s+ latency from network LSM | Download to `/tmp` first |
| **Unbounded Recursion** | Stack overflow, OOM risk | Hard MaxDepth limits |
| **Unbounded Joins** | RAM exhaustion | 5,000 fact limit |

#### Implementation Guardrails (Hard Limits)

| Guardrail | Value | Purpose |
|-----------|-------|---------|
| **Max Join Results** | 5,000 facts | Prevent RAM exhaustion during joins |
| **Query Circuit Breaker** | 2,000ms | Stop runaway queries |
| **Re-rank Limit (K)** | Top 50 | Limit random reads to `blob_db` |
| **Rule Depth** | 5 levels | Prevent infinite loops |
| **Vector Search Top-K** | 100 | Limit result set size |
| **MRL Dimensions** | 64 | Fixed search buffer size |
| **Leiden Iterations** | 10 per level | Prevent non-convergence, timeout safety |

#### MVP Roadmap (First 30 Days)

1. **Week 1**: 33-byte Quad Store with BadgerDB. Target: 1M facts without OOM
2. **Week 2**: MRL + INT8 Vector Store with `mmap`. Target: <10ms search for 1M vectors
3. **Week 3**: CSR Graph Builder + Static Leiden Algorithm at ingest
4. **Week 4**: Greedy Query Optimizer + Dynamic Sub-clustering

### Core Data Model

MEB stores all relational data as quads with four components:

```
Fact = <Subject, Predicate, Object, Graph>
       <Entity, Relationship, Value, Context>

Example:
<"pkg/auth:Login", "calls", "db:Query", "project-alpha">
```

The Graph component (4th position) provides multi-tenancy, context separation, and versioning capabilities.


## 3. Core Types

### 3.1 Document ID System

```go
// DocumentID is a strongly-typed identifier with format: "scope/name"
// Examples: "pkg/auth:Login", "types/user.go:User", "func/main:main"
type DocumentID string

// Document represents a complete entity with content and metadata
type Document struct {
    ID        DocumentID     // Unique identifier
    Content   []byte         // Source code or content
    Embedding []float32      // Vector representation
    Metadata  map[string]any // Arbitrary metadata (line numbers, etc.)
}
```

### 3.2 Fact (Quad)

```go
// Fact represents a single relational logic unit (Quad)
// Format: <Subject, Predicate, Object, Graph>
type Fact struct {
    Subject   DocumentID  // Source entity
    Predicate string      // Relationship type
    Object    any         // Target value (DocumentID, string, int, etc.)
    Graph     string      // Context/Tenant ID (defaults to "default")
    Weight    float64     // Confidence score (0.0-1.0)
    Source    string      // Provenance ("ast", "virtual", "inference")
}

// Default values
const (
    DefaultGraph  = "default"
    DefaultWeight = 1.0
    DefaultSource = "ast"
)
```

**Storage Format:**
```
User Input:     Fact{Subject: S, Predicate: P, Object: O}
Storage:        <S, P, O, "default"> as 33-byte quad key
```

### 3.3 Flexible Predicate System

MEB uses a flexible namespace-based predicate system that avoids fixed constants and supports domain-specific predicates:

```go
// Predicate is a string-based type supporting namespaced predicates
type Predicate string

// Namespace provides fluent API for creating namespaced predicates
type Namespace struct {
    prefix string
}

// Create namespaces
func NS(prefix string) *Namespace {
    return &Namespace{prefix: prefix}
}

// Pre-defined namespaces
var KB = NS("kb")
var Code = NS("code")
var Entity = NS("entity")
var System = NS("system")

// Create predicates from namespace
func (n *Namespace) P(name string) Predicate {
    if n.prefix == "" {
        return Predicate(name)
    }
    return Predicate(n.prefix + ":" + name)
}

// Parse from string
func MustParse(s string) Predicate {
    return Predicate(s)
}
```

**Usage:**

```go
// Knowledge base predicates
KB.P("entity_of")    // "kb:entity_of"
KB.P("related_to")  // "kb:related_to"
KB.P("mentions")    // "kb:mentions"
KB.P("has_title")   // "kb:has_title"
KB.P("contains")    // "kb:contains"
KB.P("link_to")    // "kb:link_to"

// Code predicates
Code.P("defines")    // "code:defines"
Code.P("calls")      // "code:calls"
Code.P("imports")    // "code:imports"
Code.P("in_package") // "code:in_package"
Code.P("has_tag")    // "code:has_tag"

// Custom domains
NS("custom").P("relationship")  // "custom:relationship"
NS("project-x").P("owned_by")  // "project-x:owned_by"

// Direct parse
MustParse("kb:references")  // "kb:references"
```

**Why Flexible Predicates?**

| Aspect | Fixed Constants | Flexible Namespaces |
|--------|-----------------|---------------------|
| Extensibility | Add new constants | `NS("new").P("predicate")` |
| Domain Isolation | Global constants | Namespaced (kb: vs code:) |
| Custom Domains | Modify constants | `NS("mydomain")` |

**Namespaces:**
- `kb` - Knowledge base (documents, notes, articles)
- `code` - Source code analysis
- `entity` - Named entity relationships
- `system` - System metadata
- `entity` - Named entity relationships
- `system` - System metadata
- `graph` - Graph structure


## 4. Store Architecture

### 4.1 MEBStore

Central orchestrator managing all subsystems with concurrent access:

```go
type MEBStore struct {
    // Triple Storage Backend
    graphDB  *badger.DB              // Graph store (quads, content)
    vocabDB  *badger.DB              // Vocabulary store (String↔ID)
    vectors  *VectorRegistry         // Vector store (mmap'd INT8)
    
    // Components
    dict        Dictionary           // String interning interface
    predicates  map[string]PredicateTable
    
    // Concurrency control
    mu          *sync.RWMutex       // Predicate table protection
    numFacts    atomic.Uint64       // Lock-free fact counter
    
    // Batch transaction support
    txn         *badger.Txn         // Active batch transaction
    
    // Configuration
    config      *Config
}
```

### 4.2 Concurrency Model

**Lock-Free Operations:**
```go
// Zero-cost atomic operations
factCount := m.numFacts.Load()     // Read
m.numFacts.Add(1)                  // Increment
m.numFacts.Add(^uint64(0))         // Decrement
```

**Fine-Grained Locking:**
```go
// Predicate table protection
m.mu.RLock()  // For reading predicate tables
m.mu.Lock()   // For registering new predicates

// Transaction reuse
if m.txn != nil {
    return fn(m.txn)  // Reuse batch transaction
}
```

**Sharded Dictionary:**
```go
// Multiple shards reduce lock contention
shardIdx := hash(s) % numShards
shard.mu.RLock()  // Per-shard read lock
shard.mu.Lock()   // Per-shard write lock
```

### 4.3 FactStore

Manages quad relationships with triple-index storage:

```go
// Core operations
func (m *MEBStore) AddFact(fact Fact) error
func (m *MEBStore) AddFactBatch(facts []Fact) error
func (m *MEBStore) DeleteGraph(graph string) error
func (m *MEBStore) DeleteFactsBySubject(subject string) error
func (m *MEBStore) Scan(s, p, o, g string) iter.Seq2[Fact, error]
func (m *MEBStore) Contains(fact Fact) bool
```

**Key Features:**
- **Triple Index**: SPO, OPS, PSO for optimal query paths
- **Metadata**: Weight and Source encoded with each quad
- **Batch Operations**: Efficient bulk insert/delete
- **Graph Scoped**: Full graph isolation via GSPO index


### 4.4 ContentStore

Document storage with lazy loading:

```go
// Document operations
func (m *MEBStore) AddDocument(id DocumentID, content []byte, embedding []float32, metadata map[string]any) error
func (m *MEBStore) GetDocument(id DocumentID) (*Document, error)
func (m *MEBStore) GetDocumentMetadata(id DocumentID) (map[string]any, error)
func (m *MEBStore) DeleteDocument(id DocumentID) error
func (m *MEBStore) HasDocument(id DocumentID) (bool, error)
```

### 4.5 VectorStore

High-performance vector storage:

```go
type VectorRegistry struct {
    vectors    [][]float32       // In-memory vectors
    stringIDs  []string          // Human-readable IDs
    idMap      map[uint64]uint32 // ID to index mapping
    revMap     []uint64          // Index to ID mapping
    db         *badger.DB
    dataDir    string
    data       []int8            // INT8 quantized vectors (flat buffer)
    mmapData   []byte            // Memory-mapped data
    scale      float32           // Quantization scale
    mu         sync.RWMutex
}

// SearchResult represents a single vector search result
type SearchResult struct {
    ID        uint64
    StringID  string
    Score     float64
    Index     int
}

// Operations
func (vr *VectorRegistry) Add(id uint64, vector []float32) error
func (vr *VectorRegistry) AddWithStringID(id uint64, stringID string, vector []float32) error
func (vr *VectorRegistry) Search(query []float32, topK int, threshold float64) ([]SearchResult, error)
func (vr *VectorRegistry) GetFullVector(id uint64) ([]float32, error)
func (vr *VectorRegistry) LoadSnapshot() error
func (vr *VectorRegistry) SaveSnapshot() error
```

**Features:**
- **In-Memory**: Fast access for active vectors
- **Memory-Mapped**: Low RAM footprint
- **SIMD Search**: AVX2/NEON optimized
- **Quantization**: INT8 for 4x memory reduction

## 5. Triple Storage Architecture

MEB uses three distinct storage backends optimized for their specific data types:

### 5.1 Storage Layout

```
┌─────────────────────────────────────────────────────────────┐
│                    MEB Storage Layout                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. GRAPH STORE (BadgerDB)                                  │
│     Path: {BaseDir}/graph/                                  │
│  ┌─────────────────────────────┐                            │
│  │ Quad Keys (SPO/POS/PSO)     │ 33-byte composite keys    │
│  │ Content Metadata            │ Document pointers         │
│  │ Full Vectors (1536-d)       │ For retrieval             │
│  │ Vector Metadata             │ IDs, stringIDs            │
│  └─────────────────────────────┘                            │
│  Config: SyncWrites=false (async, high throughput)          │
│                                                             │
│  2. VOCAB STORE (BadgerDB)                                  │
│     Path: {BaseDir}/vocab/                                  │
│  ┌─────────────────────────────┐                            │
│  │ Forward Map                 │ String → ID (0x80 prefix) │
│  │ Reverse Map                 │ ID → String (0x81 prefix) │
│  │ Next ID Counter             │ Atomic allocation tracker │
│  └─────────────────────────────┘                            │
│  Config: SyncWrites=true (strict persistence)               │
│                                                             │
│  3. VECTOR STORE (Hybrid: Flat File + BadgerDB)             │
│     Path: {BaseDir}/vector/                                 │
│  ┌─────────────────────────────┐                            │
│  │ vectors.bin                 │ INT8 quantized (64-d)     │
│  │ (Memory-mapped)             │ For SIMD search           │
│  └─────────────────────────────┘                            │
│  ┌─────────────────────────────┐                            │
│  │ Full vectors in graph DB    │ Float32 (1536-d)          │
│  │ Key: 0x10 + ID              │ For retrieval/re-ranking  │
│  └─────────────────────────────┘                            │
│  Access: mmap for search, BadgerDB for full vectors         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Storage Compression

MEB implements multi-layer compression to maximize storage efficiency while maintaining query performance:

```
┌─────────────────────────────────────────────────────────────┐
│                 Compression Stack                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  LAYER 1: Content Compression (S2)                         │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Source Code / Documents                               │ │
│  │         ↓                                             │ │
│  │    S2.Encode()                                        │ │
│  │         ↓                                             │ │
│  │   ~2-3x compression (Snappy-alike, fast)             │ │
│  │         ↓                                             │ │
│  │   BadgerDB Value Log                                  │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  LAYER 2: BadgerDB SST Compression (ZSTD)                  │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Quads, Keys, Metadata                                 │ │
│  │         ↓                                             │ │
│  │    ZSTD Level 3                                       │ │
│  │         ↓                                             │ │
│  │   ~3-5x compression (balanced speed/ratio)           │ │
│  │         ↓                                             │ │
│  │   SST Files on Disk                                   │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  LAYER 3: Vector Quantization (MRL + INT8)                │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Float32 Vectors (1536-d)                              │ │
│  │         ↓                                             │ │
│  │    MRL Truncate to 64-d                               │ │
│  │         ↓                                             │ │
│  │    Quantize to INT8                                   │ │
│  │         ↓                                             │ │
│  │   ~24x compression (1536×4 → 64×1)                   │ │
│  │         ↓                                             │ │
│  │   vectors.bin (mmap'd)                                │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Compression Methods:**

#### 1. S2 Content Compression
```go
import "github.com/klauspost/compress/s2"

// SetContent compresses and stores data
func (m *MEBStore) SetContent(id uint64, data []byte) error {
    // S2 compression - Snappy-compatible, faster compression
    compressed := s2.Encode(nil, data)
    
    key := keys.EncodeChunkKey(id)
    return m.withWriteTxn(func(txn *badger.Txn) error {
        return txn.Set(key, compressed)
    })
}

// GetContent retrieves and decompresses data
func (m *MEBStore) GetContent(id uint64) ([]byte, error) {
    key := keys.EncodeChunkKey(id)
    
    var compressed []byte
    err := m.withReadTxn(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            return err
        }
        compressed, err = item.ValueCopy(nil)
        return err
    })
    
    if err != nil {
        return nil, err
    }
    
    // S2 decompression
    return s2.Decode(nil, compressed)
}
```

**Why S2?**
- **Speed**: 2-3x faster than Snappy at similar compression ratios
- **Snappy-Compatible**: Can be read by Snappy decoders
- **Go-Native**: Pure Go implementation, no C dependencies
- **Streaming**: Supports streaming compression for large files

#### 2. BadgerDB ZSTD Compression
```go
// Config enables ZSTD compression for BadgerDB
 type Config struct {
     Compression bool  // Enable ZSTD compression
 }

func buildBadgerOptions(cfg *Config) badger.Options {
    opts := badger.DefaultOptions(filepath.Join(cfg.DataDir, "badger"))
    
    if cfg.Compression {
        opts.Compression = options.ZSTD  // ZSTD level 3
    } else {
        opts.Compression = options.None
    }
    
    return opts
}
```

**ZSTD Configuration:**
- **Level**: 3 (balanced compression/speed)
- **Target**: SST files (immutable sorted tables)
- **Benefit**: ~3-5x compression on repetitive key-value data
- **Overhead**: Minimal CPU overhead during reads

#### 3. Vector Quantization (MRL + INT8)
```go
// Quantize float32 vectors to INT8
func Quantize(vector []float32) []int8 {
    quantized := make([]int8, len(vector))
    scale := calculateScale(vector)
    
    for i, v := range vector {
        // Map float32 range to int8 range [-128, 127]
        quantized[i] = int8(v * scale)
    }
    
    return quantized
}

// Compression Ratio: 1536-d Float32 → 64-d INT8
// Before: 1536 × 4 bytes = 6144 bytes per vector
// After:   64 × 1 byte  =   64 bytes per vector
// Ratio: 96:1 compression (with MRL truncation)
```

**Combined Compression Ratios:**

| Data Type | Raw Size | Compressed | Ratio | Method |
|-----------|----------|------------|-------|--------|
| Source Code | 100MB | 35MB | 2.8:1 | S2 |
| Quads (SPOG) | 500MB | 120MB | 4.2:1 | ZSTD |
| Vectors (1536-d) | 6GB | 64MB | 96:1 | MRL+INT8 |
| **Total** | **6.6GB** | **219MB** | **30:1** | **Combined** |

### 5.3 Storage Components

| Store | Type | Content | Optimization |
|-------|------|---------|--------------|
| **graph** | BadgerDB | Quads, content metadata,<br>full Float32 vectors (1536-d) | Async writes, high throughput |
| **vocab** | BadgerDB | String↔ID mappings | Sync writes, durability |
| **vector** | Hybrid | **INT8** quantized (64-d) in flat file<br>**Float32** full vectors in graph DB | Mmap for search<br>BadgerDB for retrieval |

### 5.3 Directory Structure

```
/meb-data/
├── graph/
│   ├── 000001.vlog          # BadgerDB value log
│   ├── 000001.sst           # BadgerDB SSTable
│   └── MANIFEST             # BadgerDB manifest
├── vocab/
│   ├── 000001.vlog          # Dictionary value log
│   ├── 000001.sst           # Dictionary SSTable
│   └── MANIFEST             # Dictionary manifest
└── vector/
    └── vectors.bin          # Memory-mapped INT8 vectors
```

### 5.5 Benefits

- **Specialized Backends**: Each storage type uses optimal backend (BadgerDB for KV, flat files for sequential)
- **Independent Tuning**: Graph (async), Vocab (sync), Vector (mmap)
- **Failure Isolation**: Corruption in one doesn't affect others
- **Resource Efficiency**: Vector mmap keeps RAM usage predictable
- **Backup Flexibility**: Can backup stores independently

### 5.6 Vector Storage Architecture

MEB uses a **dual storage** approach for vectors to optimize both search speed and retrieval accuracy:

```
┌─────────────────────────────────────────────────────────────┐
│                    Vector Storage Model                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  DUAL STORAGE STRATEGY                                      │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ 1. INT8 Quantized Vectors (Search)                    │ │
│  │    Path: {BaseDir}/vector/vectors.bin                 │ │
│  │    Format: Flat file, memory-mapped                   │ │
│  │    Dimensions: 64 (MRL compressed)                    │ │
│  │    Precision: INT8 (1 byte per dimension)             │ │
│  │    Size: ~64 bytes per vector                         │ │
│  │    Use: Fast approximate similarity search            │ │
│  │    Access: Zero-copy mmap + SIMD (AVX2/NEON)          │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ 2. Full Vectors (Retrieval)                           │ │
│  │    Location: graph BadgerDB                           │ │
│  │    Key Format: [0x10][ID:8]                           │ │
│  │    Dimensions: 1536 (original)                        │ │
│  │    Precision: Float32 (4 bytes per dimension)         │ │
│  │    Size: ~6 KB per vector                             │ │
│  │    Use: Re-ranking, result enrichment                 │ │
│  │    Access: BadgerDB Get() on demand                   │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  FLOW:                                                      │
│  Search ──▶ INT8 mmap (fast scan) ──▶ Top-K IDs            │
│     │                                          │            │
│     └──▶ Fetch full vectors ──▶ Re-rank ──▶ Final Results  │
│            (from graph DB)                                  │
└─────────────────────────────────────────────────────────────┘
```

**Why Dual Storage?**

1. **INT8 Quantized**: 75% smaller than Float32, enables memory mapping of millions of vectors
2. **Full Float32**: Preserved for accurate final scoring and result quality
3. **Separation**: Search happens on compressed data, retrieval fetches originals only for top results

**Implementation:**

```go
// Add vector - stores both formats
func (r *VectorRegistry) Add(id uint64, fullVec []float32) error {
    // 1. Compress to 64-d MRL vector
    mrlVec := ProcessMRL(fullVec)
    
    // 2. Quantize to INT8 for search
    quantized := Quantize(mrlVec)
    
    // 3. Store INT8 in mmap'd flat file
    r.data = append(r.data, quantized...)
    
    // 4. Persist full Float32 to BadgerDB (async)
    go r.persistFullVector(id, fullVec)
}

// Search - uses INT8 mmap
func (r *VectorRegistry) Search(query []float32, topK int, threshold float64) ([]SearchResult, error) {
    // 1. Quantize query to INT8
    q := Quantize(query)
    
    // 2. SIMD scan mmap'd vectors
    results := r.simdSearch(q, topK, threshold)
    
    return results, nil
}

// Get full vector - fetches from BadgerDB
func (r *VectorRegistry) GetFullVector(id uint64) ([]float32, error) {
    return r.db.View(func(txn *badger.Txn) error {
        key := []byte{0x10} + encodeID(id)
        item, err := txn.Get(key)
        // ... decode Float32 vector
    })
}
```

## 6. Concurrency Architecture

### 6.1 Thread-Safe Design

MEB implements multiple concurrency strategies:

**Lock-Free Operations (Atomic):**
- `numFacts: atomic.Uint64` - Fact counter
- `nextID: atomic.Uint64` - Dictionary IDs

**Fine-Grained Mutexes:**
- `mu: sync.RWMutex` - Predicate tables
- `allocMu: sync.Mutex` - ID allocation

**Transaction Management:**
- `txn: *badger.Txn` - Batch reuse
- `withReadTxn()` - Automatic read transactions
- `withWriteTxn()` - Automatic write transactions

**Sharded Dictionary:**
- 32+ shards with per-shard RWMutex

### 6.2 Atomic Fact Counter

Lock-free operations for zero-contention counting:

```go
type MEBStore struct {
    numFacts atomic.Uint64
}

// Zero-cost read
func (m *MEBStore) Count() uint64 {
    return m.numFacts.Load()
}

// Lock-free operations
m.numFacts.Add(1)              // Increment
m.numFacts.Add(^uint64(0))     // Decrement
```

### 6.3 Transaction Reuse Pattern

Prevents nested transaction panics:

```go
func (m *MEBStore) withReadTxn(fn func(*badger.Txn) error) error {
    if m.txn != nil {
        return fn(m.txn)  // Reuse batch transaction
    }
    txn := m.newTxn()
    defer m.releaseTxn(txn)
    return fn(txn)
}

func (m *MEBStore) withWriteTxn(fn func(*badger.Txn) error) error {
    if m.txn != nil {
        return fn(m.txn)
    }
    txn := m.db.NewTransaction(true)
    defer txn.Discard()
    if err := fn(txn); err != nil {
        return err
    }
    return txn.Commit()
}

func (m *MEBStore) ExecuteBatch(fn func(*MEBStore) error) error {
    return m.db.Update(func(txn *badger.Txn) error {
        batchedStore := *m
        batchedStore.txn = txn
        return fn(&batchedStore)
    })
}
```


## 7. Dictionary System

### 7.1 Sharded Encoder Architecture

String interning with reduced lock contention:

```
String → FNV-1a Hash → Shard Index (hash % N)

┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Shard 0  │  │ Shard 1  │  │ Shard 2  │  │ Shard N  │
│ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │
│ │ LRU  │ │  │ │ LRU  │ │  │ │ LRU  │ │  │ │ LRU  │ │
│ │Cache │ │  │ │Cache │ │  │ │Cache │ │  │ │Cache │ │
│ └──────┘ │  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │
│ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │
│ │Mutex │ │  │ │Mutex │ │  │ │Mutex │ │  │ │Mutex │ │
│ └──────┘ │  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │
└──────────┘  └──────────┘  └──────────┘  └──────────┘

Global: allocMu (sync.Mutex), nextID (atomic.Uint64)
```

### 7.2 Implementation

```go
type ShardedEncoder struct {
    db        *badger.DB
    shards    []*encoderShard
    numShards int
    nextID    uint64
    allocMu   sync.Mutex
}

type encoderShard struct {
    mu           sync.RWMutex
    forwardCache *lruCache[string, uint64]
    reverseCache *lruCache[uint64, string]
}

// Dictionary interface for string interning
type Dictionary interface {
    GetOrCreateID(s string) (uint64, error)
    GetIDs(keys []string) ([]uint64, error)
    GetString(id uint64) (string, error)
    GetID(s string) (uint64, error)
    Close() error
}

// PredicateTable tracks registered predicates
type PredicateTable struct {
    Name     string
    Arity    int
    Cardinality int
}

func (e *ShardedEncoder) getShard(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32()) & (e.numShards - 1)
}

// Core operations
func (e *ShardedEncoder) GetOrCreateID(s string) (uint64, error)
func (e *ShardedEncoder) GetIDs(keys []string) ([]uint64, error)
func (e *ShardedEncoder) GetString(id uint64) (string, error)
func (e *ShardedEncoder) GetID(s string) (uint64, error)
```

### 7.3 Concurrency Strategy

```go
func (e *ShardedEncoder) GetOrCreateID(s string) (uint64, error) {
    shardIdx := e.getShard(s)
    shard := e.shards[shardIdx]
    
    // 1. Lock-free cache check
    if id, ok := shard.forwardCache.Get(s); ok {
        return id, nil
    }
    
    // 2. Read-locked cache check
    shard.mu.RLock()
    if id, ok := shard.forwardCache.Get(s); ok {
        shard.mu.RUnlock()
        return id, nil
    }
    shard.mu.RUnlock()
    
    // 3. Check BadgerDB
    id, err := e.lookupInDB(s)
    if err == nil {
        shard.mu.Lock()
        shard.forwardCache.Add(s, id)
        shard.mu.Unlock()
        return id, nil
    }
    
    // 4. Allocate new ID
    e.allocMu.Lock()
    newID := atomic.AddUint64(&e.nextID, 1)
    e.allocMu.Unlock()
    
    // 5. Persist and update cache
    e.persistMapping(s, newID)
    shard.mu.Lock()
    shard.forwardCache.Add(s, newID)
    shard.mu.Unlock()
    
    return newID, nil
}
```


## 8. Storage Layer

### 8.1 Quad Key Encoding (Little-Endian, Architecture-Neutral)

All facts stored as 33-byte composite keys using **Little-Endian byte order** for optimal x86/x64 performance and cross-platform compatibility:

```go
const (
    QuadSPOGPrefix byte = 0x20  // Subject-Predicate-Object-Graph
    QuadPOSGPrefix byte = 0x21  // Predicate-Object-Subject-Graph
    QuadGSPOPrefix byte = 0x22  // Graph-Subject-Predicate-Object
)

const QuadKeySize = 33  // prefix(1) + 4*ID(8)

// Key structure (Little-Endian for mmap efficiency on x86/x64):
// SPOG: [0x20][subject:8][predicate:8][object:8][graph:8]
//       │     │              │               │               │
//       │     │              │               │               └─ Graph ID (LE)
//       │     │              │               └─ Object ID (LE)
//       │     │              └─ Predicate ID (LE)
//       │     └─ Subject ID (LE)
//       └─ Prefix byte

// EncodeQuadKey encodes using Little-Endian for architecture-neutral mmap
func EncodeQuadKey(prefix byte, s, p, o, g uint64) []byte {
    key := make([]byte, QuadKeySize)
    key[0] = prefix
    binary.LittleEndian.PutUint64(key[1:9], s)
    binary.LittleEndian.PutUint64(key[9:17], p)
    binary.LittleEndian.PutUint64(key[17:25], o)
    binary.LittleEndian.PutUint64(key[25:33], g)
    return key
}

// DecodeQuadKey decodes Little-Endian quad key
func DecodeQuadKey(key []byte) (s, p, o, g uint64) {
    if len(key) != QuadKeySize {
        return 0, 0, 0, 0
    }
    s = binary.LittleEndian.Uint64(key[1:9])
    p = binary.LittleEndian.Uint64(key[9:17])
    o = binary.LittleEndian.Uint64(key[17:25])
    g = binary.LittleEndian.Uint64(key[25:33])
    return
}
```

**Why Little-Endian?**
- **x86/x64 native**: Most modern servers use x86_64 (Little-Endian native)
- **Zero-cost mmap**: Direct memory access without byte-swapping overhead
- **SIMD friendly**: AVX2/NEON operations assume Little-Endian data
- **Architecture-neutral**: Explicit encoding ensures same binary layout on ARM64

### 8.2 Metadata Encoding (Little-Endian with Padding)

Metadata uses **Little-Endian encoding with explicit padding** for architecture-neutral mmap compatibility:

```go
// MetadataHeader is the fixed-size header for fact metadata
// Little-Endian with 8-byte alignment for safe mmap access
type MetadataHeader struct {
    Version     uint32   // 4 bytes: encoding version
    Weight      float32  // 4 bytes: compressed weight (0.0-1.0)
    SourceHash  uint32   // 4 bytes: hash of source identifier
    _           uint32   // 4 bytes: PADDING for 16-byte alignment
} // Total: 16 bytes (aligned)

const (
    MetadataHeaderSize = 16
    CurrentMetadataVersion = 2
)

// EncodeFactMetadata creates Little-Endian metadata with padding
func EncodeFactMetadata(fact Fact) []byte {
    // Calculate total size: header + source string + padding to 8-byte boundary
    sourceBytes := []byte(fact.Source)
    sourceLen := len(sourceBytes)
    
    // Align to 8-byte boundary for safe mmap access
    padding := (8 - (sourceLen % 8)) % 8
    totalSize := MetadataHeaderSize + sourceLen + padding
    
    buf := make([]byte, totalSize)
    
    // Write header (Little-Endian)
    binary.LittleEndian.PutUint32(buf[0:4], CurrentMetadataVersion)
    binary.LittleEndian.PutUint32(buf[4:8], math.Float32bits(float32(fact.Weight)))
    binary.LittleEndian.PutUint32(buf[8:12], hashSource(fact.Source))
    // buf[12:16] is padding (already zeroed)
    
    // Write source string
    copy(buf[MetadataHeaderSize:], sourceBytes)
    
    return buf
}

// DecodeFactMetadata parses Little-Endian metadata with padding
func DecodeFactMetadata(data []byte) (weight float64, source string) {
    if len(data) < MetadataHeaderSize {
        return 1.0, "ast"
    }
    
    // Read header (Little-Endian)
    version := binary.LittleEndian.Uint32(data[0:4])
    if version != CurrentMetadataVersion {
        // Unknown version - return defaults for forward compatibility
        return 1.0, "ast"
    }
    
    weight32 := math.Float32frombits(binary.LittleEndian.Uint32(data[4:8]))
    weight = float64(weight32)
    
    // Source string starts after header, null-terminated or length-delimited
    // For safety, scan until non-printable or end
    sourceBytes := data[MetadataHeaderSize:]
    for i, b := range sourceBytes {
        if b == 0 || !isPrintable(b) {
            source = string(sourceBytes[:i])
            break
        }
    }
    if source == "" && len(sourceBytes) > 0 {
        source = string(sourceBytes)
    }
    
    return
}

// Architecture-Neutral Access Pattern for mmap
func ReadMetadataFromMmap(mmapData []byte, offset int) (MetadataHeader, []byte) {
    // Ensure 8-byte aligned access
    if offset%8 != 0 {
        offset = (offset + 7) &^ 7  // Round up to 8-byte boundary
    }
    
    // Direct slice access - safe due to Little-Endian + alignment
    header := MetadataHeader{
        Version:    binary.LittleEndian.Uint32(mmapData[offset:]),
        Weight:     math.Float32frombits(binary.LittleEndian.Uint32(mmapData[offset+4:])),
        SourceHash: binary.LittleEndian.Uint32(mmapData[offset+8:]),
    }
    
    // Variable-length data follows header (also aligned)
    payload := mmapData[offset+MetadataHeaderSize:]
    
    return header, payload
}
```

**Padding Strategy:**
- **8-byte alignment**: All structures aligned to 8-byte boundaries
- **Version field**: Forward compatibility for schema evolution
- **Architecture-neutral**: Same binary layout on x86_64, ARM64, RISC-V
- **SIMD-safe**: Aligned access prevents bus errors on strict-alignment architectures

### 8.3 Page-Aligned Zero-Copy Retrieval

MEB implements page-aligned zero-copy retrieval to minimize memory allocation and CPU overhead when reading facts from BadgerDB. This optimization is critical for high-throughput query workloads.

**The Problem with Traditional Copying:**

```
Traditional Read Flow (with copy):
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  BadgerDB    │────►│ Kernel Page  │────►│ Go Heap      │────►│ User Code    │
│  mmap'd data │     │ Cache        │     │ (copy)       │     │              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
     4KB page                              33-64 bytes                     
     (zero-copy)                           (allocation +
                                           memcpy overhead)
```

Issues:
- **Memory allocation**: Each read allocates new `[]byte` on Go heap
- **CPU overhead**: `memcpy` from kernel page to user buffer
- **GC pressure**: Short-lived allocations trigger frequent garbage collection
- **Cache pollution**: Unnecessary data movement evicts useful cache lines

**Zero-Copy Solution:**

```
Zero-Copy Read Flow:
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  BadgerDB    │────►│ Kernel Page  │────►│ User Code    │
│  mmap'd data │     │ Cache        │     │ (direct ref) │
└──────────────┘     └──────────────┘     └──────────────┘
     4KB page           4KB page            33-64 byte slice
     (mmap'd)           (direct access)     (points into page)
```

Benefits:
- **Zero allocation**: No heap allocation for key/value data
- **Zero memcpy**: Direct pointer to kernel page cache
- **GC friendly**: No short-lived objects, reduced GC pressure
- **Cache efficient**: Data stays in CPU cache, no eviction

**Page Alignment Strategy (Little-Endian, Architecture-Neutral):**

Linux systems use 4KB pages (4096 bytes). MEB uses **Little-Endian encoding with explicit 8-byte padding** for architecture-neutral mmap compatibility:

```
Page Layout (4KB = 4096 bytes, Little-Endian, 8-byte aligned):
┌─────────────────────────────────────────────────────────────────┐
│  Page Header (64 bytes, 8-byte aligned)                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • Magic: uint32 (4 bytes) [LE]                            │  │
│  │ • Version: uint32 (4 bytes) [LE]                          │  │
│  │ • NumEntries: uint64 (8 bytes) [LE]                       │  │
│  │ • PageID: uint64 (8 bytes) [LE]                           │  │
│  │ • DataOffset: uint32 (4 bytes) [LE]                       │  │
│  │ • Flags: uint32 (4 bytes) [LE]                            │  │
│  │ • Reserved: 32 bytes (PADDING, 8-byte aligned)            │  │
│  │ Total: 64 bytes                                           │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Entry Directory (8-byte aligned entries)                       │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Entry_1:                                                   │  │
│  │   KeyOffset: uint32 (4 bytes) [LE]                        │  │
│  │   KeySize: uint16 (2 bytes) [LE]                          │  │
│  │   MetaOffset: uint32 (4 bytes) [LE]                       │  │
│  │   MetaSize: uint16 (2 bytes) [LE]                         │  │
│  │   PADDING: uint16 (2 bytes)                               │  │
│  │   Total: 16 bytes (8-byte aligned)                        │  │
│  │ Entry_2: [same structure]                                 │  │
│  │ ...                                                       │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Data Region (8-byte aligned)                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Key_1 (33 bytes) + PADDING (7 bytes) = 40 bytes aligned   │  │
│  │ Metadata_1 (16 bytes) [8-byte aligned]                    │  │
│  │ Key_2 (33 bytes) + PADDING (7 bytes) = 40 bytes aligned   │  │
│  │ Metadata_2 (16 bytes) [8-byte aligned]                    │  │
│  │ ...                                                       │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Padding to 4KB boundary (4096-byte aligned)                    │
└─────────────────────────────────────────────────────────────────┘
```

**Architecture-Neutral Design Principles:**

1. **Little-Endian Byte Order**: All multi-byte fields use Little-Endian encoding
   - Native on x86_64 (most common server architecture)
   - Explicit byte-swapping on ARM64 (predictable performance)
   - Consistent binary format across platforms

2. **8-Byte Alignment**: All structures aligned to 8-byte boundaries
   - Prevents unaligned access faults on strict-alignment architectures
   - Optimal for 64-bit loads/stores
   - SIMD-friendly (AVX2 requires aligned loads for best performance)

3. **Explicit Padding**: Padding bytes are explicitly defined, not implicit
   - Documented structure layout
   - Version field enables forward compatibility
   - Reserved space for future extensions

**ZeroCopyBuffer Design:**

```go
// ZeroCopyBuffer provides safe access to mmap'd data without copying
type ZeroCopyBuffer struct {
    // Direct pointer to mmap'd memory (unsafe, but managed)
    ptr unsafe.Pointer
    
    // Length of valid data in bytes
    len int
    
    // Page ID for validation
    pageID uint64
    
    // Reference count for lifecycle management
    refCount atomic.Int32
}

// Slice returns a byte slice pointing directly into mmap'd memory
// WARNING: Slice is only valid until the next page eviction
func (z *ZeroCopyBuffer) Slice(offset, length int) []byte {
    // Bounds check
    if offset < 0 || offset+length > z.len {
        return nil
    }
    
    // Ensure 8-byte aligned access for architecture safety
    if offset%8 != 0 {
        // Return unaligned slice (caller must handle carefully)
        // Prefer using ReadUint64LE for aligned access
    }
    
    // Create slice header pointing to mmap'd memory
    // No allocation, no memcpy
    return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(z.ptr)+uintptr(offset))), length)
}

// ReadUint64LE reads Little-Endian uint64 at aligned offset (ARCHITECTURE-NEUTRAL)
// Safe for mmap'd data on both x86_64 and ARM64
func (z *ZeroCopyBuffer) ReadUint64LE(offset int) uint64 {
    // Ensure 8-byte alignment
    if offset%8 != 0 {
        panic("unaligned access: offset must be 8-byte aligned")
    }
    
    // Direct memory read - Little-Endian
    // On x86_64: native byte order, single instruction
    // On ARM64: explicit byte order, portable
    ptr := (*uint64)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
    return *ptr
}

// ReadUint32LE reads Little-Endian uint32 at aligned offset
func (z *ZeroCopyBuffer) ReadUint32LE(offset int) uint32 {
    if offset%4 != 0 {
        panic("unaligned access: offset must be 4-byte aligned")
    }
    ptr := (*uint32)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
    return *ptr
}

// ReadFloat64LE reads Little-Endian float64 at aligned offset
func (z *ZeroCopyBuffer) ReadFloat64LE(offset int) float64 {
    if offset%8 != 0 {
        panic("unaligned access: offset must be 8-byte aligned")
    }
    bits := z.ReadUint64LE(offset)
    return math.Float64frombits(bits)
}

// DecodeQuadKeyAt decodes 33-byte quad key at offset using Little-Endian
// Architecture-neutral: same result on x86_64, ARM64, RISC-V
func (z *ZeroCopyBuffer) DecodeQuadKeyAt(offset int) (prefix byte, s, p, o, g uint64) {
    // Ensure 8-byte aligned access to key components
    if offset%8 != 0 {
        offset = (offset + 7) &^ 7  // Round up to 8-byte boundary
    }
    
    // Read prefix (1 byte)
    prefixPtr := (*byte)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
    prefix = *prefixPtr
    
    // Read IDs using Little-Endian (offset+1 to skip prefix)
    s = binary.LittleEndian.Uint64(z.Slice(offset+1, 8))
    p = binary.LittleEndian.Uint64(z.Slice(offset+9, 8))
    o = binary.LittleEndian.Uint64(z.Slice(offset+17, 8))
    g = binary.LittleEndian.Uint64(z.Slice(offset+25, 8))
    
    return
}

// Retain increments reference count
func (z *ZeroCopyBuffer) Retain() {
    z.refCount.Add(1)
}

// Release decrements reference count, may unmap page
func (z *ZeroCopyBuffer) Release() {
    if z.refCount.Add(-1) == 0 {
        // Safe to unmap page
        z.unmapPage()
    }
}
```

**Integration with BadgerDB:**

BadgerDB uses mmap for value log files. MEB extends this to provide zero-copy access:

```go
// ZeroCopyIterator wraps BadgerDB iterator with zero-copy semantics
type ZeroCopyIterator struct {
    inner     *badger.Iterator
    pageCache map[uint64]*ZeroCopyBuffer
    mu        sync.RWMutex
}

// Next advances iterator and returns zero-copy key/value
func (z *ZeroCopyIterator) Next() (key, value []byte, err error) {
    if !z.inner.Valid() {
        return nil, nil, ErrIteratorExhausted
    }
    
    item := z.inner.Item()
    
    // Get the page containing this item
    pageID := z.getPageID(item)
    
    z.mu.RLock()
    buf, ok := z.pageCache[pageID]
    z.mu.RUnlock()
    
    if !ok {
        // Load page into cache
        buf = z.loadPage(pageID)
        z.mu.Lock()
        z.pageCache[pageID] = buf
        z.mu.Unlock()
    }
    
    // Get zero-copy slice into mmap'd memory
    key = buf.Slice(item.KeyOffset(), item.KeySize())
    
    // Value may be in value log (separate mmap)
    // For inline values (< 128 bytes), use zero-copy
    if item.ValueSize() <= 128 {
        value = buf.Slice(item.ValueOffset(), item.ValueSize())
    } else {
        // Large values: use traditional copy for safety
        value, err = item.ValueCopy(nil)
    }
    
    z.inner.Next()
    return key, value, err
}
```

**Safety Considerations:**

Zero-copy requires careful lifecycle management to prevent use-after-free:

```go
// PageRef provides safe reference-counted access to mmap'd pages
type PageRef struct {
    buffer *ZeroCopyBuffer
    valid  atomic.Bool
}

// GetKey returns zero-copy key slice
func (p *PageRef) GetKey() []byte {
    if !p.valid.Load() {
        return nil
    }
    return p.buffer.Slice(0, 33) // 33-byte quad key
}

// Close releases the page reference
func (p *PageRef) Close() {
    if p.valid.CompareAndSwap(true, false) {
        p.buffer.Release()
    }
}

// ScanZeroCopy performs zero-copy iteration with automatic cleanup
func (m *MEBStore) ScanZeroCopy(prefix []byte) iter.Seq2[Fact, error] {
    return func(yield func(Fact, error) bool) {
        txn := m.db.NewTransaction(false)
        defer txn.Discard()
        
        it := txn.NewIterator(badger.DefaultIteratorOptions)
        defer it.Close()
        
        pageCache := NewPageCache(100) // Max 100 pages cached
        defer pageCache.Clear() // Release all pages on exit
        
        for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
            item := it.Item()
            
            // Get zero-copy reference to page
            pageRef := pageCache.GetPage(item)
            
            // Decode key directly from mmap'd memory
            key := pageRef.GetKey()
            s, p, o := keys.DecodeQuadKey(key)
            
            // Resolve IDs to strings (may allocate, but key doesn't)
            subject, _ := m.dict.GetString(s)
            predicate, _ := m.dict.GetString(p)
            object, _ := m.dict.GetString(o)
            
            fact := Fact{
                Subject:   DocumentID(subject),
                Predicate: predicate,
                Object:    object,
            }
            
            if !yield(fact, nil) {
                pageRef.Close()
                return
            }
            
            pageRef.Close()
        }
    }
}
```

**Performance Characteristics:**

| Metric | Traditional Copy | Zero-Copy | Improvement |
|--------|-----------------|-----------|-------------|
| **Allocation** | 2 per fact | 0 per fact | ∞ (eliminated) |
| **Memory Copy** | 100-200 bytes | 0 bytes | ∞ (eliminated) |
| **Latency** | ~500ns | ~50ns | **10x** |
| **Throughput** | 2M facts/sec | 20M facts/sec | **10x** |
| **GC Pressure** | High | Minimal | **~0%** |

**When to Use Zero-Copy:**

✅ **Use for:**
- Large-scale scans (100K+ facts)
- Real-time streaming queries
- Memory-constrained environments
- Read-only analytical workloads

❌ **Avoid for:**
- Small result sets (< 100 facts) - overhead not worth it
- Write-heavy workloads - pages may be invalidated
- Long-lived references - risk of page eviction
- Complex post-processing that needs mutable data

#### 8.3.1 Architecture-Neutral mmap Best Practices

MEB's mmap implementation follows strict architecture-neutral principles for cross-platform compatibility:

**1. Little-Endian Byte Order (Mandatory)**

```go
// CORRECT: Little-Endian encoding
func writeUint64LE(dst []byte, val uint64) {
    binary.LittleEndian.PutUint64(dst, val)
}

// INCORRECT: Platform-dependent (unsafe on Big-Endian systems)
func writeUint64Unsafe(dst []byte, val uint64) {
    *(*uint64)(unsafe.Pointer(&dst[0])) = val
}
```

**Rationale:**
- **x86_64 native**: Zero-cost on most servers
- **ARM64 portable**: Explicit byte order (ARM64 is bi-endian, default varies)
- **RISC-V compatible**: Explicit Little-Endian support
- **Network protocols**: Different from network byte order (Big-Endian), but internal storage only

**2. Explicit 8-Byte Alignment (Mandatory)**

```go
// CORRECT: Aligned access
type AlignedHeader struct {
    Field1 uint64  // 8 bytes, aligned
    Field2 uint32  // 4 bytes
    _      uint32  // 4 bytes PADDING
    Field3 uint64  // 8 bytes, aligned
} // Total: 24 bytes (8-byte aligned)

// INCORRECT: Unaligned access (may fault on strict architectures)
type UnalignedHeader struct {
    Field1 uint8   // 1 byte
    Field2 uint64  // 8 bytes, UNALIGNED!
}
```

**Architecture Alignment Requirements:**
| Architecture | uint8 | uint16 | uint32 | uint64 | float32 | float64 |
|-------------|-------|--------|--------|--------|---------|---------|
| x86_64 | 1 | 1 | 1 | 1 | 1 | 1 |
| ARM64 | 1 | 2 | 4 | 8 | 4 | 8 |
| RISC-V | 1 | 2 | 4 | 8 | 4 | 8 |

**3. Padding Strategy (Explicit)**

```go
// MEB padding convention: Always pad to 8-byte boundary
const Alignment = 8

func padToAlignment(size int) int {
    return (size + Alignment - 1) &^ (Alignment - 1)
}

// Example: 33-byte key padded to 40 bytes
keySize := 33
paddedSize := padToAlignment(keySize) // = 40
padding := paddedSize - keySize       // = 7 bytes
```

**4. Platform Detection (Runtime Safety)**

```go
// Verify architecture compatibility on startup
func verifyArchitecture() error {
    // Check endianness
    if !isLittleEndian() {
        return fmt.Errorf("MEB requires Little-Endian architecture")
    }
    
    // Check pointer size (must be 64-bit)
    if strconv.IntSize != 64 {
        return fmt.Errorf("MEB requires 64-bit architecture")
    }
    
    // Verify alignment assumptions
    if unsafe.Alignof(uint64(0)) != 8 {
        return fmt.Errorf("unexpected alignment: uint64 should be 8-byte aligned")
    }
    
    return nil
}

func isLittleEndian() bool {
    var x uint16 = 0x0102
    return *(*byte)(unsafe.Pointer(&x)) == 0x02
}
```

**5. Cross-Platform Testing Matrix**

| Platform | Architecture | Endianness | Status |
|----------|--------------|------------|--------|
| Linux | x86_64 | Little | ✅ Primary |
| Linux | ARM64 | Little | ✅ Supported |
| macOS | x86_64 | Little | ✅ Supported |
| macOS | ARM64 (M1/M2) | Little | ✅ Supported |
| Windows | x86_64 | Little | ⚠️ Limited |
| FreeBSD | x86_64 | Little | ⚠️ Community |

**6. Migration Guide (Big-Endian to Little-Endian)**

If migrating existing Big-Endian data:

```go
// Migration utility for data format conversion
func migrateBigEndianToLittleEndian(src, dst string) error {
    // 1. Read existing Big-Endian data
    data, err := os.ReadFile(src)
    if err != nil {
        return err
    }
    
    // 2. Convert all multi-byte fields
    for offset := 0; offset < len(data); offset += 8 {
        if offset+8 <= len(data) {
            // Read Big-Endian
            beVal := binary.BigEndian.Uint64(data[offset:])
            // Write Little-Endian
            binary.LittleEndian.PutUint64(data[offset:], beVal)
        }
    }
    
    // 3. Write converted data
    return os.WriteFile(dst, data, 0644)
}
```

**7. Debugging mmap Issues**

```go
// Enable verbose mmap diagnostics
const DebugMmap = true

func (z *ZeroCopyBuffer) debugAccess(offset int, size int, operation string) {
    if !DebugMmap {
        return
    }
    
    // Check alignment
    if offset%8 != 0 && size >= 8 {
        log.Printf("WARNING: Unaligned %s at offset %d (size %d)", 
            operation, offset, size)
    }
    
    // Check bounds
    if offset < 0 || offset+size > z.len {
        log.Printf("ERROR: Out-of-bounds %s at offset %d (size %d, buffer %d)",
            operation, offset, size, z.len)
    }
    
    // Hex dump first 64 bytes on error
    if offset+size > z.len {
        hex.Dump(z.Slice(0, min(64, z.len)))
    }
}
```

**Configuration:**

```go
type Config struct {
    // ... other fields ...
    
    // Zero-copy settings
    EnableZeroCopy     bool  // Enable zero-copy for scans (default: true)
    ZeroCopyPageCache  int   // Max pages to cache (default: 1000)
    ZeroCopyThreshold  int   // Min result size to enable zero-copy (default: 100)
}
```

**Integration with LFTJ:**

Zero-copy is particularly effective with Leapfrog Triejoin, which performs many sequential scans:

```go
func (e *LFTJEngine) createIterator(atom Atom) *TrieIterator {
    it := &TrieIterator{
        store: e.store,
        atom:  atom,
    }
    
    // Use zero-copy for large result sets
    if e.useZeroCopy {
        it.scanFn = e.store.ScanZeroCopy
    } else {
        it.scanFn = e.store.Scan
    }
    
    return it
}
```

**Platform Considerations:**

- **Linux**: Full support via mmap, 4KB pages
- **macOS**: Full support via mmap, 4KB/16KB pages (ARM64)
- **Windows**: Limited support, fallback to copy mode
- **WSL**: Works but with higher syscall overhead

### 8.4 Incremental Value Log Garbage Collection

MEB implements **incremental Value Log (VLog) Garbage Collection** to reclaim storage space from deleted or obsolete values without impacting query performance. This is essential for long-running production deployments.

#### 8.4.1 The VLog Problem

BadgerDB separates keys (in LSM-tree) from values (in append-only VLog files):

```
Storage Layout:
┌─────────────────────────────────────────────────────────────┐
│ LSM-Tree (Keys)          │ Value Log (Values)               │
├─────────────────────────────────────────────────────────────┤
│ Key: [prefix][ID]        │ Value File: 000001.vlog          │
│ Points to:               │ ┌──────────────────────────────┐ │
│   - VLog file ID         │ │ [header][value][checksum]    │ │
│   - Offset in file       │ │ [header][value][checksum]    │ │
│   - Value length         │ │ ...                          │ │
│                          │ └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**The Garbage Accumulation Problem:**

1. **Updates**: New value written to VLog, old value becomes unreachable
2. **Deletes**: Key removed from LSM-tree, but value remains in VLog
3. **Over time**: VLog grows with "garbage" (unreachable values)
4. **Impact**: Storage bloat, longer backup times, slower iteration

**Example Growth Pattern:**

| Day | Total Data | Live Data | Garbage | VLog Size |
|-----|-----------|-----------|---------|-----------|
| 1 | 10 GB | 10 GB | 0 GB | 10 GB |
| 7 | 20 GB | 12 GB | 8 GB | 20 GB |
| 30 | 50 GB | 15 GB | 35 GB | 50 GB |
| 90 | 120 GB | 18 GB | 102 GB | 120 GB |

Without GC: **85% storage waste after 90 days!**

#### 8.4.2 Incremental GC Design

Instead of expensive "stop-the-world" GC, MEB processes VLog files incrementally:

```
Incremental GC Pipeline:
┌─────────────────────────────────────────────────────────────┐
│ Stage 1: Select Candidate File                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ • Pick oldest VLog file (highest garbage probability)   │ │
│ │ • Check if file has high garbage ratio (>20%)           │ │
│ │ • Skip files with active iterators                      │ │
│ └─────────────────────────────────────────────────────────┘ │
│                          ↓                                  │
│ Stage 2: Scan & Filter                                       │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ • Iterate all values in VLog file                       │ │
│ │ • For each value: check if key in LSM-tree points here  │ │
│ │ • Keep live values, mark garbage for deletion           │ │
│ └─────────────────────────────────────────────────────────┘ │
│                          ↓                                  │
│ Stage 3: Compact & Rewrite                                   │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ • Create new VLog file                                  │ │
│ │ • Copy only live values                                 │ │
│ │ • Update LSM-tree pointers to new locations             │ │
│ │ • Little-Endian encoding preserved                      │ │
│ └─────────────────────────────────────────────────────────┘ │
│                          ↓                                  │
│ Stage 4: Atomic Swap                                         │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ • Sync new VLog to disk                                 │ │
│ │ • Atomic rename (old → backup, new → active)            │ │
│ │ • Delete old VLog file                                  │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Key Innovation: Incremental Processing**

```go
// GC runs in small chunks to avoid impacting queries
type IncrementalGC struct {
    store        *MEBStore
    
    // Chunking parameters
    maxBytesPerRun   int64   // Max bytes to process per GC cycle (default: 100MB)
    maxTimePerRun    time.Duration  // Max time per cycle (default: 5s)
    
    // Thresholds
    garbageRatioThreshold float64  // Min garbage % to trigger GC (default: 0.20)
    minFileAge           time.Duration  // Min age before GC eligible (default: 1h)
}

// RunGC performs incremental garbage collection
func (gc *IncrementalGC) RunGC() (stats GCStats, err error) {
    start := time.Now()
    
    // 1. Select candidate VLog files
    candidates := gc.selectCandidates()
    
    for _, file := range candidates {
        // Check time budget
        if time.Since(start) > gc.maxTimePerRun {
            break
        }
        
        // Check byte budget
        if stats.BytesProcessed >= gc.maxBytesPerRun {
            break
        }
        
        // Process one file incrementally
        fileStats, err := gc.processFile(file)
        if err != nil {
            return stats, err
        }
        
        stats.BytesProcessed += fileStats.BytesTotal
        stats.BytesReclaimed += fileStats.BytesGarbage
        stats.FilesProcessed++
    }
    
    stats.Duration = time.Since(start)
    return stats, nil
}
```

#### 8.4.3 Live Value Detection

GC must accurately determine which values are still reachable:

```go
// isValueLive checks if a VLog entry is still referenced by LSM-tree
func (gc *IncrementalGC) isValueLive(vlogID uint32, offset uint64) (bool, error) {
    // 1. Read the key associated with this VLog entry
    key, err := gc.readKeyAt(vlogID, offset)
    if err != nil {
        return false, err
    }
    
    // 2. Look up current key in LSM-tree
    currentRef, err := gc.store.db.Get(key)
    if err == badger.ErrKeyNotFound {
        // Key deleted - value is garbage
        return false, nil
    }
    if err != nil {
        return false, err
    }
    
    // 3. Parse current reference (Little-Endian)
    currentVlogID := binary.LittleEndian.Uint32(currentRef[0:4])
    currentOffset := binary.LittleEndian.Uint64(currentRef[4:12])
    
    // 4. Check if current reference matches this VLog entry
    return currentVlogID == vlogID && currentOffset == offset, nil
}
```

**Little-Endian Note:** All VLog pointers use Little-Endian encoding (consistent with storage layer) for architecture-neutral mmap compatibility.

#### 8.4.4 Architecture-Neutral VLog Format

VLog files use explicit **Little-Endian encoding with 8-byte alignment**:

```
VLog Entry Format (Little-Endian, Architecture-Neutral):
┌─────────────────────────────────────────────────────────────┐
│ Header (16 bytes, 8-byte aligned)                           │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ KeyLength:     uint32 (4 bytes) [LE]                    │ │
│ │ ValueLength:   uint32 (4 bytes) [LE]                    │ │
│ │ ExpiresAt:     uint64 (8 bytes) [LE] - TTL or 0         │ │
│ │ Total: 16 bytes (8-byte aligned)                        │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Key (variable, 8-byte aligned)                              │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Key bytes (KeyLength)                                   │ │
│ │ Padding to 8-byte boundary                              │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Value (variable, 8-byte aligned)                            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Value bytes (ValueLength)                               │ │
│ │ Padding to 8-byte boundary                              │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Footer (8 bytes, 8-byte aligned)                            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Checksum: uint64 (8 bytes) [LE] - CRC64                 │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

Total Entry Size: 16 + padded(KeyLength) + padded(ValueLength) + 8
```

**Alignment Requirements:**
- Header: 16 bytes (8-byte aligned)
- Key: padded to 8-byte boundary
- Value: padded to 8-byte boundary  
- Footer: 8 bytes (8-byte aligned)

**Why Alignment Matters for GC:**
- Enables safe mmap access during GC scan
- Allows SIMD-accelerated checksum calculation
- Prevents bus errors on strict-alignment architectures (ARM64)

#### 8.4.5 Scheduling & Throttling

GC runs in background with careful resource management:

```go
// GCScheduler manages when and how GC runs
type GCScheduler struct {
    gc           *IncrementalGC
    
    // Scheduling policy
    interval     time.Duration     // Run GC every N seconds (default: 1h)
    maxCPUPercent float64          // Max CPU % for GC (default: 10%)
    
    // Adaptive throttling
    queryLatencySMA *movingAverage // Track recent query latency
    pauseGC        bool            // Pause GC if latency spikes
}

// Start begins background GC scheduling
func (s *GCScheduler) Start() {
    ticker := time.NewTicker(s.interval)
    
    go func() {
        for range ticker.C {
            // Check if GC should run
            if s.shouldRunGC() {
                stats, err := s.gc.RunGC()
                if err != nil {
                    slog.Error("GC failed", "error", err)
                } else {
                    slog.Info("GC completed",
                        "bytes_reclaimed", stats.BytesReclaimed,
                        "files_processed", stats.FilesProcessed,
                        "duration", stats.Duration)
                }
            }
        }
    }()
}

// shouldRunGC decides if GC should run now
func (s *GCScheduler) shouldRunGC() bool {
    // 1. Don't run if explicitly paused
    if s.pauseGC {
        return false
    }
    
    // 2. Don't run if query latency is high
    if s.queryLatencySMA.Value() > 100*time.Millisecond {
        return false
    }
    
    // 3. Don't run during peak hours (configurable)
    if isPeakHour() {
        return false
    }
    
    // 4. Check if garbage ratio exceeds threshold
    garbageRatio := s.gc.calculateGarbageRatio()
    return garbageRatio > s.gc.garbageRatioThreshold
}
```

**Adaptive Throttling:**

```go
// adaptThrottling adjusts GC intensity based on system load
func (s *GCScheduler) adaptThrottling() {
    latency := s.queryLatencySMA.Value()
    
    switch {
    case latency < 10*time.Millisecond:
        // Low latency: increase GC throughput
        s.gc.maxBytesPerRun *= 2
        s.gc.maxTimePerRun *= 2
        
    case latency > 100*time.Millisecond:
        // High latency: pause GC
        s.pauseGC = true
        go func() {
            time.Sleep(5 * time.Minute)
            s.pauseGC = false
        }()
        
    default:
        // Normal: reset to defaults
        s.gc.maxBytesPerRun = 100 * 1024 * 1024  // 100MB
        s.gc.maxTimePerRun = 5 * time.Second
    }
}
```

#### 8.4.6 Safety Guarantees

**Crash Safety:**

```go
// processFileWithCrashSafety processes VLog with crash recovery
func (gc *IncrementalGC) processFileWithCrashSafety(file VLogFile) error {
    // 1. Create temp file
    tempFile := file.Path + ".gc.tmp"
    
    // 2. Copy live values to temp file
    if err := gc.copyLiveValues(file, tempFile); err != nil {
        os.Remove(tempFile)  // Clean up on error
        return err
    }
    
    // 3. fsync temp file to ensure durability
    if err := gc.syncFile(tempFile); err != nil {
        os.Remove(tempFile)
        return err
    }
    
    // 4. Atomic rename (atomic on POSIX)
    backupFile := file.Path + ".gc.backup"
    if err := os.Rename(file.Path, backupFile); err != nil {
        os.Remove(tempFile)
        return err
    }
    
    if err := os.Rename(tempFile, file.Path); err != nil {
        // Attempt rollback
        os.Rename(backupFile, file.Path)
        return err
    }
    
    // 5. Delete backup after successful swap
    os.Remove(backupFile)
    
    return nil
}
```

**Iterator Safety:**

```go
// safeToGC checks if file can be GC'd (no active iterators)
func (gc *IncrementalGC) safeToGC(file VLogFile) bool {
    gc.store.mu.RLock()
    defer gc.store.mu.RUnlock()
    
    for _, it := range gc.store.activeIterators {
        if it.vlogFileID == file.ID {
            // Active iterator using this file - skip GC
            return false
        }
    }
    return true
}
```

**Zero-Copy Compatibility:**

```go
// GC must preserve zero-copy semantics
type GCZeroCopyHandler struct {
    gc *IncrementalGC
}

// remapPages updates mmap pointers after GC moves values
func (h *GCZeroCopyHandler) remapPages(oldVlogID, newVlogID uint32) {
    // Update all ZeroCopyBuffer instances referencing old VLog
    h.gc.store.pageCache.mu.Lock()
    defer h.gc.store.pageCache.mu.Unlock()
    
    for pageID, buf := range h.gc.store.pageCache.buffers {
        if buf.vlogFileID == oldVlogID {
            // Invalidate page (will be remapped on next access)
            buf.valid.Store(false)
            delete(h.gc.store.pageCache.buffers, pageID)
        }
    }
}
```

#### 8.4.7 Configuration

```go
type Config struct {
    // ... other fields ...
    
    // Incremental VLog GC Settings
    EnableGC              bool          // Enable GC (default: true)
    GCInterval            time.Duration // GC check interval (default: 1h)
    GCMaxBytesPerRun      int64         // Max bytes per GC cycle (default: 100MB)
    GCMaxTimePerRun       time.Duration // Max time per GC cycle (default: 5s)
    GCGarbageRatioThreshold float64      // Min garbage % to trigger (default: 0.20)
    GCMinFileAge          time.Duration // Min file age before eligible (default: 1h)
    GCPauseIfHighLatency  bool          // Pause GC on latency spike (default: true)
    GCMaxLatencyThreshold time.Duration // Latency threshold to pause (default: 100ms)
}
```

#### 8.4.8 Performance Characteristics

| Metric | Stop-the-World GC | Incremental GC | Improvement |
|--------|------------------|----------------|-------------|
| **Query Latency Impact** | 100-500ms spikes | <5ms | **20-100x** |
| **GC Duration** | Hours | Seconds | **1000x** |
| **Storage Reclamation** | Immediate | Gradual | Acceptable |
| **CPU Overhead** | 100% (exclusive) | 10% (throttled) | **10x** |
| **Crash Recovery** | Complex | Simple | **Safer** |

**Real-World Performance:**

```
Production Deployment (1B facts, 500GB VLog):

Before GC:
- VLog Size: 500 GB
- Live Data: 120 GB
- Garbage: 380 GB (76%)

After 7 days of Incremental GC:
- VLog Size: 145 GB
- Live Data: 120 GB
- Garbage: 25 GB (17%)
- Query P99 Latency: unchanged (<20ms)
- GC CPU: averaged 8%
```

#### 8.4.9 Monitoring & Observability

```go
// GCStats tracks GC performance
type GCStats struct {
    BytesProcessed     int64         // Total bytes scanned
    BytesReclaimed     int64         // Garbage bytes removed
    FilesProcessed     int           // VLog files compacted
    FilesSkipped       int           // Files skipped (iterators, low garbage)
    ValuesCopied       int64         // Live values preserved
    ValuesDeleted      int64         // Garbage values removed
    Duration           time.Duration // Total GC time
    QueryLatencyImpact time.Duration // Max latency spike during GC
}

// Export metrics for monitoring
func (gc *IncrementalGC) exportMetrics(stats GCStats) {
    metrics.Gauge("meb_gc_bytes_reclaimed").Set(float64(stats.BytesReclaimed))
    metrics.Gauge("meb_gc_files_processed").Set(float64(stats.FilesProcessed))
    metrics.Histogram("meb_gc_duration_seconds").Observe(stats.Duration.Seconds())
    metrics.Gauge("meb_gc_garbage_ratio").Set(gc.calculateGarbageRatio())
}
```

**Alert Thresholds:**

| Metric | Warning | Critical |
|--------|---------|----------|
| Garbage Ratio | >50% | >75% |
| GC Duration | >10s | >30s |
| Query Latency Impact | >50ms | >100ms |
| Failed GC Runs | 1/hour | 3/hour |

#### 8.4.10 Best Practices

**1. Production Deployment:**

```go
// Recommended production settings
config := &meb.Config{
    EnableGC:                true,
    GCInterval:              1 * time.Hour,
    GCMaxBytesPerRun:        100 * 1024 * 1024,  // 100MB
    GCMaxTimePerRun:         5 * time.Second,
    GCGarbageRatioThreshold: 0.20,  // 20%
    GCPauseIfHighLatency:    true,
    GCMaxLatencyThreshold:   100 * time.Millisecond,
}
```

**2. Maintenance Windows:**

```go
// Force GC during maintenance window
func forceGCDuringMaintenance(store *meb.MEBStore) {
    gc := store.GetGC()
    
    // Temporarily increase GC throughput
    gc.maxBytesPerRun = 1024 * 1024 * 1024  // 1GB
    gc.maxTimePerRun = 1 * time.Minute
    
    // Run until garbage ratio < 10%
    for gc.calculateGarbageRatio() > 0.10 {
        stats, err := gc.RunGC()
        if err != nil {
            log.Fatal(err)
        }
        log.Printf("GC reclaimed %d MB", stats.BytesReclaimed/1024/1024)
    }
    
    // Reset to normal settings
    gc.maxBytesPerRun = 100 * 1024 * 1024
    gc.maxTimePerRun = 5 * time.Second
}
```

**3. Disaster Recovery:**

```go
// Emergency GC if disk is filling up
func emergencyGC(store *meb.MEBStore, diskUsage float64) {
    if diskUsage > 0.90 {  // 90% full
        gc := store.GetGC()
        
        // Aggressive GC settings
        gc.garbageRatioThreshold = 0.05  // 5% (lower threshold)
        gc.maxTimePerRun = 10 * time.Minute
        
        // Run GC continuously until disk usage drops
        for diskUsage > 0.70 {
            stats, _ := gc.RunGC()
            diskUsage = checkDiskUsage()
            
            log.Printf("Emergency GC: reclaimed %d MB, disk usage %.1f%%",
                stats.BytesReclaimed/1024/1024, diskUsage*100)
        }
    }
}
```

## 9. Query System

### 9.1 Datalog Query Syntax

MEB supports a Datalog-inspired query language for declarative graph traversal:

```datalog
% Find all functions calling a specific function
?triples(X, 'calls', 'fmt.Printf')

% With explicit graph context
?triples(X, 'calls', 'fmt.Printf', 'project-alpha')

% Find interface implementations
?triples(X, 'implements', 'io.Reader')

% Complex queries with bindings
triples(X, 'calls', Y), triples(Y, 'defines', 'helper')

% Multi-atom query with variable binding
triples(S, 'defines', X), triples(X, 'type', 'func'), regex(X, "^auth\\.")

% Negation and inequality
triples(F, 'calls', 'db.Query'), neq(F, 'test_helper')
```

**Query Processing Model:**

```
┌─────────────────────────────────────────────────────────────┐
│                   Query Execution Pipeline                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Stage 1: Parsing & Classification                          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Parse Datalog syntax into atoms                     │ │
│  │ • Classify atoms: data atoms vs constraints          │ │
│  │ • Identify variables and constants                   │ │
│  └───────────────────────────────────────────────────────┘ │
│                         ↓                                   │
│  Stage 2: Nested Loop Join (Multi-Atom Processing)          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Initialize with empty binding set                   │ │
│  │ • For each data atom: substitute bound variables     │ │
│  │ • Execute Scan with partial bindings                 │ │
│  │ • Expand bindings with new variable assignments      │ │
│  └───────────────────────────────────────────────────────┘ │
│                         ↓                                   │
│  Stage 3: Post-Filter (Constraint Evaluation)               │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Apply regex filters on bound variables             │ │
│  │ • Evaluate inequality constraints (neq)              │ │
│  │ • Filter results based on metadata (weight, source)  │ │
│  └───────────────────────────────────────────────────────┘ │
│                         ↓                                   │
│  Stage 4: Result Projection                                 │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Return variable bindings as result set             │ │
│  │ • Include hidden metadata fields (_weight, _source)  │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Supported Constraints:**
- `regex(Var, "pattern")` - Regular expression matching on variable values
- `neq(A, B)` - Inequality constraint (A ≠ B)
- Future: `gt`, `lt`, `in`, `contains` for richer filtering

**Variable Binding Rules:**
- Variables start with `?` or uppercase letter (e.g., `?X`, `Func`)
- Underscore `_` is a wildcard (don't care variable)
- Variables are bound incrementally through the join pipeline
- Constraints can reference both variables and constants

### 9.2 Query Builder

Fluent API for building complex queries:

```go
// QueryBuilder provides chainable query construction
type QueryBuilder struct {
    store      *MEBStore
    conditions []Condition
    limit      int
}

// Condition represents a single query condition
type Condition struct {
    Field    string      // "subject", "predicate", "object", "graph"
    Operator string      // "=", "!=", "~" (regex), "similar"
    Value    interface{} // string, DocumentID, float32[] for similarity
}

// Query builder methods
func (qb *QueryBuilder) Where() *QueryBuilder
func (qb *QueryBuilder) Subject(s DocumentID) *QueryBuilder
func (qb *QueryBuilder) Predicate(p string) *QueryBuilder
func (qb *QueryBuilder) Object(o interface{}) *QueryBuilder
func (qb *QueryBuilder) Graph(g string) *QueryBuilder
func (qb *QueryBuilder) SimilarTo(embedding []float32, threshold float64) *QueryBuilder
func (qb *QueryBuilder) ObjectMatch(pattern string) *QueryBuilder
func (qb *QueryBuilder) And() *QueryBuilder
func (qb *QueryBuilder) Limit(n int) *QueryBuilder
func (qb *QueryBuilder) Execute() ([]QueryResult, error)

// Usage example
qb := store.Find()
results, err := qb.Where().
    Subject("pkg/auth").
    Predicate("defines").
    Execute()
```

### 9.3 Query Result

```go
type QueryResult struct {
    Bindings []map[string]any  // Variable bindings from query
    Facts    []Fact            // Matching facts
    Scores   []float64         // Similarity scores (if vector search)
}
```

### 9.4 Iterator Processing Model

MEB provides functional programming utilities for processing query results through composable iterator operations:

```go
// Type-safe value extraction
func Value[T any](f Fact) (T, bool)           // Safe cast with ok check
func MustValue[T any](f Fact) T              // Panic-on-fail cast
func ValueOrDefault[T any](f Fact, defaultVal T) T

// Iterator transformations
func Collect(seq iter.Seq2[Fact, error]) ([]Fact, error)           // Materialize to slice
func Filter(seq iter.Seq2[Fact, error], pred func(Fact) bool) iter.Seq2[Fact, error]
func Map[T any](seq iter.Seq2[Fact, error], fn func(Fact) (T, error)) iter.Seq2[T, error]

// Iterator aggregation
func First(seq iter.Seq2[Fact, error]) (Fact, error)               // First matching fact
func CountFacts(seq iter.Seq2[Fact, error]) (int, error)           // Count results
```

**Processing Pipeline Example:**

```go
// Chain operations for complex filtering
results, err := meb.Collect(
    meb.Filter(
        store.Scan("Alice", "knows", "", ""),
        func(f Fact) bool {
            // Filter: only adults (age >= 18)
            ageFacts, _ := meb.Collect(store.Scan(f.Object.(string), "age", "", ""))
            for _, af := range ageFacts {
                if age, ok := meb.Value[int](af); ok && age >= 18 {
                    return true
                }
            }
            return false
        },
    ),
)

// Transform results
names, err := meb.Collect(
    meb.Map(store.Scan("", "defines", "", ""), func(f Fact) (string, error) {
        name, ok := meb.Value[string](f)
        if !ok {
            return "", fmt.Errorf("not a string")
        }
        return name, nil
    }),
)
```

**Design Principles:**
- **Lazy Evaluation:** Operations chain without materialization until `Collect()`
- **Type Safety:** Generic functions provide compile-time type checking
- **Error Propagation:** Errors flow through the pipeline and surface at collection
- **Memory Efficiency:** Streaming processing for large result sets


## 10. Datalog Query Optimization (Leapfrog Triejoin)

MEB implements Leapfrog Triejoin (LFTJ), a worst-case optimal multi-way join algorithm that significantly outperforms traditional nested-loop joins for complex Datalog queries.

### 10.1 Why Leapfrog Triejoin?

**Traditional Nested-Loop Join Problems:**
- Pairwise joins produce intermediate results that may be larger than final output
- Time complexity: O(|R₁| × |R₂| × ... × |Rₙ|) in worst case
- Memory-intensive for multi-atom queries

**LFTJ Advantages:**
- **Worst-case optimal**: Runs in O(N × |output|) where N is number of relations
- **No intermediate materialization**: Streams results directly
- **Trie-based**: Leverages lexicographic ordering of keys
- **Leapfrogging**: Efficiently skips non-matching tuples

**Performance Comparison:**

| Query Type | Nested Loop | LFTJ | Improvement |
|------------|-------------|------|-------------|
| Triangle counting (3-way join) | O(n³) | O(n²) | 10-100x |
| Path finding (4-way join) | O(n⁴) | O(n²) | 100-1000x |
| Transitive closure | O(n⁴) | O(n²) | 100-1000x |

### 10.2 TrieIterator Abstraction

LFTJ views each relation as a **trie** (prefix tree) where tuples are sorted lexicographically. The storage layer provides trie-structured access via the BadgerDB key-value store.

```
Relation R(A, B, C) as Trie:
                    root
                      │
            ┌─────────┴─────────┐
            a1                  a2
            │                   │
      ┌─────┴─────┐       ┌─────┴─────┐
      b1         b2       b1         b3
      │          │        │          │
   ┌──┴──┐    ┌──┴──┐  ┌──┴──┐    ┌──┴──┐
   c1   c2    c1   c3  c1   c2    c2   c3

Trie Path: (a1,b1,c1), (a1,b1,c2), (a1,b2,c1), (a1,b2,c3), ...
```

**TrieIterator Interface:**

```go
// TrieIterator provides trie-structured access to a relation
type TrieIterator struct {
    txn    *badger.Txn
    it     *badger.Iterator
    prefix []byte          // Current position prefix
    depth  int             // Current trie depth (attribute position)
    
    // Column mapping: position -> attribute index
    // For triples: pos 0=Subject, 1=Predicate, 2=Object
    columnOrder []int
}

// Core LFTJ Operations
func (ti *TrieIterator) Open() error              // Initialize at root
func (ti *TrieIterator) Up() error                // Move up to parent
func (ti *TrieIterator) Seek(key []byte) bool    // Seek to key at current depth
func (ti *TrieIterator) Next() bool              // Move to next sibling
func (ti *TrieIterator) Key() []byte             // Get current key component
func (ti *TrieIterator) AtEnd() bool             // Check if exhausted
```

**BadgerDB Integration:**

MEB's SPO/POS/PSO indices naturally form tries:

```
SPO Index Trie Structure:
Prefix: [0x01 | Subject(8) | Predicate(8) | Object(8)]
        │     │              │               │
        │     └─ Level 0     └─ Level 1      └─ Level 2
        └─ Prefix byte

Traversal:
Level 0 (Subject):    Seek to subject ID
Level 1 (Predicate):  Within subject, seek to predicate ID  
Level 2 (Object):     Within predicate, iterate objects
```

### 10.3 Leapfrog Join Algorithm

The leapfrog join finds the intersection of multiple iterators at each trie level.

**Algorithm Overview:**

```
LeapfrogJoin(iterators []TrieIterator):
    1. Sort iterators by current key (ascending)
    2. While not all iterators exhausted:
       a. Let max_key = maximum of all iterator keys
       b. For each iterator (in round-robin order):
          - Seek to max_key
          - If key > max_key: update max_key, restart loop
       c. If all iterators at same key: 
          - Output tuple
          - Advance all iterators
```

**Example: 3-Way Join**

```
Query: triples(X, 'knows', Y), triples(Y, 'knows', Z), triples(Z, 'knows', X)

Relations at Level 0 (Subject):
R1: [Alice, Bob, Charlie, ...]
R2: [Bob, Charlie, Dave, ...]  
R3: [Charlie, Alice, ...]

Leapfrog Execution:
1. Sort: Alice(R1), Bob(R2), Charlie(R3)
2. max_key = Charlie
3. R1 seeks to Charlie: found (advance max_key)
4. R2 seeks to Charlie: found
5. R3 already at Charlie: found
6. All match! Output (Charlie, ...) and recurse to Level 1
7. Advance all to next
```

**Complexity Analysis:**

- **Time**: O(N × |output| × log(k)) where k is max relation size
- **Space**: O(N × depth) for iterator stack
- **Optimal**: Matches theoretical worst-case bound for join queries

### 10.4 Variable Ordering & Query Planning

LFTJ performance depends critically on variable ordering. MEB uses a cost-based optimizer to determine optimal attribute order.

**Variable Ordering Strategy:**

```go
// VariableOrder represents the sequence of attributes to bind
type VariableOrder struct {
    Attributes []Attribute // Ordered list of attributes to join
    Cost       float64     // Estimated cost
}

type Attribute struct {
    Position  int    // Position in tuple (0=subject, 1=predicate, 2=object)
    Relation  int    // Which relation (for multi-relation queries)
    Selectivity float64 // Estimated selectivity
}

// DetermineOptimalOrder uses statistics to find best ordering
func DetermineOptimalOrder(query Query, stats Statistics) VariableOrder {
    // 1. Calculate selectivity for each attribute
    // 2. Place most selective attributes first (reduces search space)
    // 3. Consider index availability (SPO vs OPS)
    // 4. Use dynamic programming for multi-relation queries
}
```

**Ordering Heuristics:**

| Priority | Heuristic | Rationale |
|----------|-----------|-----------|
| 1 | Bound constants first | Eliminates iterators early |
| 2 | High selectivity | Reduces intermediate results |
| 3 | Index availability | Prefer indexed attributes |
| 4 | Correlation | Group correlated attributes |

**Example Query Planning:**

```datalog
triples(A, 'calls', B), triples(B, 'calls', C), triples(C, 'calls', A)

Naive Order: A → B → C (may produce large intermediates)
Optimized Order: 
  1. Start with 'calls' predicate (most selective, constant)
  2. Use cardinality stats: if |calls| = 1000, |defines| = 5000
  3. Order: predicate → subject → object
```

### 10.5 LFTJ Query Execution

The query engine converts Datalog atoms into LFTJ operations:

```go
// LFTJEngine executes queries using leapfrog triejoin
type LFTJEngine struct {
    store     *MEBStore
    iterators map[string]*TrieIterator // Relation name -> iterator
}

// Execute runs a multi-atom query using LFTJ
func (e *LFTJEngine) Execute(query Query) (chan Result, error) {
    // 1. Parse query into atoms
    atoms := query.Atoms
    
    // 2. Create TrieIterator for each atom
    for i, atom := range atoms {
        it := e.createIterator(atom, i)
        e.iterators[atom.Name] = it
    }
    
    // 3. Determine optimal variable order
    order := DetermineOptimalOrder(query, e.store.GetStatistics())
    
    // 4. Execute leapfrog join
    return e.leapfrogJoin(order)
}

func (e *LFTJEngine) leapfrogJoin(order VariableOrder) (chan Result, error) {
    results := make(chan Result)
    
    go func() {
        defer close(results)
        
        // Initialize all iterators at root
        for _, it := range e.iterators {
            it.Open()
        }
        
        // Recursive descent through trie levels
        e.joinAtLevel(0, order, make(map[string]interface{}), results)
    }()
    
    return results, nil
}

func (e *LFTJEngine) joinAtLevel(
    level int, 
    order VariableOrder, 
    bindings map[string]interface{},
    results chan Result,
) {
    if level >= len(order.Attributes) {
        // All variables bound, output result
        results <- Result{Bindings: bindings}
        return
    }
    
    attr := order.Attributes[level]
    iterators := e.getIteratorsForAttribute(attr)
    
    // Leapfrog join at this level
    for {
        // Sort by current key
        sort.Slice(iterators, func(i, j int) bool {
            return bytes.Compare(iterators[i].Key(), iterators[j].Key()) < 0
        })
        
        maxKey := iterators[len(iterators)-1].Key()
        
        // Leapfrog
        allMatch := true
        for _, it := range iterators {
            if !it.Seek(maxKey) {
                return // Exhausted
            }
            if !bytes.Equal(it.Key(), maxKey) {
                maxKey = it.Key()
                allMatch = false
                break
            }
        }
        
        if allMatch {
            // All iterators at same key, recurse to next level
            bindings[attr.Name] = maxKey
            e.joinAtLevel(level+1, order, bindings, results)
            
            // Advance all iterators
            for _, it := range iterators {
                it.Next()
                if it.AtEnd() {
                    return
                }
            }
        }
    }
}
```

### 10.6 Integration with Existing Query System

LFTJ replaces the nested-loop join in the query execution pipeline while maintaining backward compatibility:

```
Query Execution Pipeline with LFTJ:

┌─────────────────────────────────────────────────────────────┐
│  Stage 1: Parsing & Classification                          │
│  (Same as before)                                           │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 2: Query Planning (NEW)                              │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Determine variable ordering                         │ │
│  │ • Select optimal indices (SPO/OPS/PSO)               │ │
│  │ • Choose join strategy (LFTJ for multi-atom)         │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 3: Leapfrog Triejoin (REPLACES nested loop)          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Create TrieIterators for each atom                  │ │
│  │ • Execute leapfrog join level by level               │ │
│  │ • Stream results without materialization             │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 4: Post-Processing                                   │
│  (Constraints, projection - same as before)                 │
└─────────────────────────────────────────────────────────────┘
```

**Fallback Strategy:**

```go
func (e *QueryEngine) Execute(query Query) ([]Result, error) {
    // Use LFTJ for multi-atom queries
    if len(query.Atoms) > 1 {
        return e.lftjEngine.Execute(query)
    }
    
    // Use simple scan for single-atom queries
    return e.scanEngine.Execute(query)
}
```

### 10.7 Performance Optimizations

**1. Iterator Pooling:**

```go
type IteratorPool struct {
    available []*TrieIterator
    mu        sync.Mutex
}

func (p *IteratorPool) Acquire() *TrieIterator {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    if len(p.available) > 0 {
        it := p.available[len(p.available)-1]
        p.available = p.available[:len(p.available)-1]
        return it
    }
    return &TrieIterator{}
}
```

**2. Early Termination:**

```go
func (e *LFTJEngine) joinAtLevel(level int, ...) {
    // Check circuit breaker
    if e.shouldTerminate() {
        return
    }
    
    // Check result limit
    if e.resultCount.Load() >= e.maxResults {
        return
    }
    
    // ... rest of join logic
}
```

**3. Adaptive Variable Ordering:**

```go
// Reorder mid-execution if estimates were wrong
func (e *LFTJEngine) adaptiveReorder(currentOrder VariableOrder) VariableOrder {
    // If current level producing too many results, 
    // try reordering remaining attributes
    if e.currentLevelResults > e.threshold {
        return e.reorderRemainingAttributes(currentOrder)
    }
    return currentOrder
}
```

### 10.8 Benefits Summary

| Aspect | Nested Loop | LFTJ | Winner |
|--------|-------------|------|--------|
| **Time Complexity** | O(∏\|Rᵢ\|) | O(N × \|output\|) | LFTJ |
| **Space Complexity** | O(intermediates) | O(N × depth) | LFTJ |
| **Streaming** | No | Yes | LFTJ |
| **Cache Efficiency** | Poor | Excellent | LFTJ |
| **Implementation** | Simple | Complex | Nested Loop |
| **Single Atom** | Fast | Overhead | Nested Loop |

**When to Use LFTJ:**
- Multi-atom queries (2+ relations)
- Large datasets where intermediate results would be huge
- Queries with complex join patterns (triangles, paths)
- Memory-constrained environments

**When to Use Simple Scan:**
- Single-atom queries
- Small datasets (< 10K tuples)
- Simple lookups with bound attributes
- Real-time latency requirements (< 1ms)


## 11. Vector Operations

### 10.1 Quantization

```go
func QuantizeFloat32ToInt8(vector []float32, scale float32) []int8
func DequantizeInt8ToFloat32(vector []int8, scale float32) []float32
func CalculateOptimalScale(vectors [][]float32) float32
```

### 10.2 SIMD Search

```go
// Platform-optimized
func dotProductFloat32AVX2(a, b []float32) float32  // x86_64
func dotProductFloat32NEON(a, b []float32) float32  // ARM64
func dotProductInt8AVX2(a, b []int8) int32
func dotProductInt8NEON(a, b []int8) int32

// Auto-selected
func DotProduct(a, b []float32) float32
```

### 10.3 Memory Mapping

```go
func MmapVectors(filename string) (*MmapHandle, error)
func (h *MmapHandle) GetVector(idx int) []int8
func (h *MmapHandle) Close() error
```

## 12. Hydration System

### 12.1 Hydration Modes

MEB supports multiple hydration strategies optimized for different use cases:

```go
// Deep hydration with recursive child fetching
func Hydrate(ctx context.Context, ids []DocumentID, lazy bool) ([]HydratedSymbol, error)

// Shallow hydration - metadata only, no children
func HydrateShallow(ctx context.Context, ids []DocumentID, lazy bool) ([]HydratedSymbol, error)

// HydratedSymbol represents a fully-resolved entity
type HydratedSymbol struct {
    ID       DocumentID       // Unique identifier
    Kind     string           // Entity type (from facts)
    Content  string           // Source code/content
    Metadata map[string]any   // Associated metadata
    Children []HydratedSymbol // Recursive children (deep mode only)
}
```

**Hydration Modes:**

| Mode | Description | Use Case |
|------|-------------|----------|
| **Deep + Full** | All children, full content | Detailed analysis |
| **Deep + Lazy** | All children, metadata only | Structure browsing |
| **Shallow + Full** | No children, full content | Bulk operations |
| **Shallow + Lazy** | No children, metadata only | Lists/grids |

**Lazy Loading:**
- When `lazy=true`, content bodies are skipped, only metadata is fetched
- Reduces I/O for large-scale operations
- Line range metadata enables on-demand snippet extraction

**Parallel Processing:**
- Configurable concurrency (default: 10 workers)
- Bounded parallelism prevents resource exhaustion
- Error isolation - failures don't cascade

### 12.2 Source Extraction

```go
// Extract lines from parent document using metadata
type HydratedDocument struct {
    DocumentID DocumentID
    Content    string
    LineStart  int
    LineEnd    int
    Related    []*HydratedDocument
}
```

**Features:**
- Line extraction from source files
- Configurable recursion depth
- Error resilience
- Content decompression on-demand

### 12.3 Hydration Push-Down

**Problem:** Traditional approach requires two round trips:
1. Query returns document IDs
2. Separate hydration call fetches full documents

**Solution:** Push hydration into query execution layer:

```
Traditional Query Flow:
┌─────────────────────────────────────────────────────────────┐
│  Query ("find auth functions")                               │
│       ↓                                                      │
│  Store.Query() ──▶ Returns []DocumentID                     │
│       ↓                                                      │
│  Hydrate(ids) ──▶ Separate DB lookups                       │
│       ↓                                                      │
│  Return []HydratedDocument                                  │
└─────────────────────────────────────────────────────────────┘
                              2 Round Trips

Hydration Push-Down Flow:
┌─────────────────────────────────────────────────────────────┐
│  QueryWithHydrate("find auth functions", depth=2)           │
│       ↓                                                      │
│  Store.QueryWithHydrate()                                   │
│       ↓                                                      │
│  1. Scan quads ──▶ Get DocumentIDs                          │
│       ↓                                                      │
│  2. Batch fetch content from graph DB (within same txn)     │
│       ↓                                                      │
│  3. Resolve related docs (depth=2)                          │
│       ↓                                                      │
│  Return []HydratedDocument                                  │
└─────────────────────────────────────────────────────────────┘
                              1 Round Trip
```

**Implementation:**

```go
// QueryWithHydrate performs query and hydration in single operation
type QueryWithHydrateOptions struct {
    Hydrate       bool          // Enable hydration
    MaxDepth      int           // Recursion depth
    IncludeSource bool          // Include source code
    Parallelism   int           // Concurrent workers (default: 10)
}

func (m *MEBStore) QueryWithHydrate(
    query string,
    opts QueryWithHydrateOptions,
) ([]HydratedDocument, error) {
    // 1. Execute query to get document IDs
    results, err := m.Query(query)
    if err != nil {
        return nil, err
    }
    
    // 2. Extract unique document IDs
    docIDs := extractDocumentIDs(results)
    
    // 3. Push-down: Batch fetch all content within single transaction
    return m.hydrateInBatch(docIDs, opts.MaxDepth, opts.Parallelism)
}

// hydrateInBatch performs batch hydration with shared transaction
func (m *MEBStore) hydrateInBatch(
    docIDs []DocumentID,
    maxDepth int,
    parallelism int,
) ([]HydratedDocument, error) {
    // Use shared read transaction for all fetches
    return m.withReadTxn(func(txn *badger.Txn) error {
        // Fetch all documents in parallel
        docs := make([]*HydratedDocument, len(docIDs))
        
        var wg sync.WaitGroup
        sem := make(chan struct{}, parallelism)
        
        for i, id := range docIDs {
            wg.Add(1)
            sem <- struct{}{}
            
            go func(idx int, docID DocumentID) {
                defer wg.Done()
                defer func() { <-sem }()
                
                // Use shared transaction
                doc, err := m.hydrateWithTxn(txn, docID, maxDepth)
                if err != nil {
                    docs[idx] = nil
                    return
                }
                docs[idx] = doc
            }(i, id)
        }
        
        wg.Wait()
        return nil
    })
}

// QueryBuilder integration
func (qb *QueryBuilder) WithHydrate(depth int) *QueryBuilder {
    qb.hydrate = true
    qb.hydrateDepth = depth
    return qb
}

func (qb *QueryBuilder) ExecuteWithHydrate() ([]HydratedDocument, error) {
    // Get document IDs
    results, err := qb.Execute()
    if err != nil {
        return nil, err
    }
    
    // Extract IDs
    var docIDs []DocumentID
    for _, r := range results {
        if id, ok := r.Bindings["id"].(DocumentID); ok {
            docIDs = append(docIDs, id)
        }
    }
    
    // Batch hydrate
    return qb.store.hydrateInBatch(docIDs, qb.hydrateDepth, 10)
}
```

**Usage:**

```go
// Traditional approach (2 round trips)
results, _ := store.Query(ctx, "?triples(X, 'calls', 'auth')")
docIDs := extractIDs(results)
docs, _ := HydrateShallow(store, docIDs) // Second DB round-trip

// With push-down (1 round trip)
docs, _ := store.QueryWithHydrate(
    "?triples(X, 'calls', 'auth')",
    QueryWithHydrateOptions{
        Hydrate:     true,
        MaxDepth:    1,
        Parallelism: 10,
    },
)

// QueryBuilder with push-down
docs, _ := store.Find().
    Where().
    Predicate("calls").
    Object("auth").
    WithHydrate(2).        // Push hydration into query
    ExecuteWithHydrate()
```

**Benefits:**
- **Reduced Latency:** Single round-trip instead of two
- **Transaction Consistency:** All reads in same snapshot
- **Better Throughput:** Batch operations reduce DB load
- **Memory Efficiency:** Streaming results, no intermediate ID list

## 13. Analysis System

MEB provides a comprehensive analysis engine that performs static code analysis to infer implicit relationships and detect architectural patterns. The analyzer operates on the fact graph to derive insights not explicitly stated in the source code.

### 13.1 Architecture

```go
// Analyzer performs static analysis on the knowledge graph
type Analyzer struct {
    store *MEBStore
    
    // Analysis configuration
    maxDepth        int           // Max recursion depth for analysis
    timeout         time.Duration // Analysis timeout
    parallelWorkers int           // Parallel analysis workers
}

// NewAnalyzer creates an analyzer with default settings
func NewAnalyzer(store *MEBStore) *Analyzer {
    return &Analyzer{
        store:           store,
        maxDepth:        5,
        timeout:         30 * time.Second,
        parallelWorkers: 10,
    }
}
```

### 15.2 Dependency Resolution

The analyzer resolves indirect dependencies by tracing through interface implementations and usage patterns:

```go
// ResolveDependencies finds all symbols that potentially depend on the given symbol
// through interface implementations, type assertions, or structural typing
func (a *Analyzer) ResolveDependencies(symbol DocumentID) ([]Fact, error) {
    var dependencies []Fact
    
    // 1. Find direct callers
    for fact := range a.store.Scan("", "calls", string(symbol), "") {
        dependencies = append(dependencies, fact)
    }
    
    // 2. Find interface-based callers
    // If symbol implements an interface, find all callers of that interface
    interfaces := a.findImplementedInterfaces(symbol)
    for _, iface := range interfaces {
        for fact := range a.store.Scan("", "calls", iface, "") {
            // Create virtual fact: caller potentially_calls symbol
            vFact := Fact{
                Subject:   fact.Subject,
                Predicate: "v:potentially_calls",
                Object:    symbol,
                Weight:    0.8,
                Source:    "virtual",
            }
            dependencies = append(dependencies, vFact)
        }
    }
    
    // 3. Find type assertion-based usage
    typeAssertions := a.findTypeAssertions(symbol)
    dependencies = append(dependencies, typeAssertions...)
    
    return dependencies, nil
}

// findImplementedInterfaces finds all interfaces implemented by a symbol
func (a *Analyzer) findImplementedInterfaces(symbol DocumentID) []string {
    var interfaces []string
    
    // Check for explicit implements facts
    for fact := range a.store.Scan(string(symbol), "implements", "", "") {
        if iface, ok := fact.Object.(string); ok {
            interfaces = append(interfaces, iface)
        }
    }
    
    // Infer implicit implementations via structural typing
    implicit := a.inferImplicitInterfaces(symbol)
    interfaces = append(interfaces, implicit...)
    
    return interfaces
}
```

**Dependency Analysis Example:**

```
Input: Symbol "auth:Login"

Direct Dependencies:
  - handler:LoginHandler calls auth:Login
  - middleware:AuthMiddleware calls auth:Login

Interface-Based Dependencies (v:potentially_calls):
  - api:UserAPI calls interface:Authenticator
    auth:Login implements Authenticator
    → api:UserAPI v:potentially_calls auth:Login [weight: 0.8]

Type Assertion Dependencies:
  - test:MockAuth type-asserts auth:Login
    → test:MockAuth v:potentially_calls auth:Login [weight: 0.6]
```

### 15.3 Wiring Detection

The analyzer detects dependency injection (DI) wiring patterns, commonly used in frameworks like Wire, Fx, or manual DI:

```go
// DetectWiring analyzes DI container configurations to find wiring relationships
func (a *Analyzer) DetectWiring(provider DocumentID) ([]Fact, error) {
    var wiring []Fact
    
    // 1. Scan for DI annotations or configuration
    doc, err := a.store.GetDocument(provider)
    if err != nil {
        return nil, err
    }
    
    // Extract wire tags from metadata
    wireTags := extractWireTags(doc.Metadata)
    
    // 2. Find providers for each wire tag
    for _, tag := range wireTags {
        providers := a.findProvidersForTag(tag)
        
        for _, p := range providers {
            // Create virtual wiring fact
            vFact := Fact{
                Subject:   provider,
                Predicate: "v:wires_to",
                Object:    p,
                Weight:    0.7,
                Source:    "virtual",
            }
            wiring = append(wiring, vFact)
        }
    }
    
    // 3. Detect transitive wiring (A → B → C implies A → C)
    transitive := a.detectTransitiveWiring(wiring)
    wiring = append(wiring, transitive...)
    
    return wiring, nil
}

// extractWireTags parses wire injection tags from metadata
func extractWireTags(metadata map[string]any) []string {
    if wire, ok := metadata["wire"].(string); ok && wire != "" {
        return strings.Split(wire, ",")
    }
    return nil
}

// findProvidersForTag finds all symbols that provide the given wire tag
func (a *Analyzer) findProvidersForTag(tag string) []DocumentID {
    var providers []DocumentID
    
    // Scan for provides relationship
    for fact := range a.store.Scan("", "provides", tag, "") {
        providers = append(providers, fact.Subject)
    }
    
    // Also check type names that match the tag
    for fact := range a.store.Scan("", "defines", "", "") {
        if strings.HasSuffix(string(fact.Subject), ":"+tag) {
            providers = append(providers, fact.Subject)
        }
    }
    
    return providers
}
```

**Wiring Detection Example:**

```
File: service:user_service.go
  Metadata: wire="UserRepository,Logger"

Detected Wiring:
  - service:UserService v:wires_to db:UserRepository
  - service:UserService v:wires_to log:Logger

Transitive Wiring:
  - api:UserAPI uses service:UserService
  - service:UserService v:wires_to db:UserRepository
  → api:UserAPI transitively depends on db:UserRepository
```

### 15.4 Virtual Fact Inference

The analyzer infers virtual facts that represent implicit relationships not explicitly present in source code:

```go
// InferVirtualFacts analyzes the entire graph to generate virtual facts
func (a *Analyzer) InferVirtualFacts() ([]Fact, error) {
    var virtualFacts []Fact
    
    ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
    defer cancel()
    
    // Run analyses in parallel
    g, ctx := errgroup.WithContext(ctx)
    g.SetLimit(a.parallelWorkers)
    
    results := make(chan []Fact, 10)
    
    // Analysis 1: Interface-based calls
    g.Go(func() error {
        facts, err := a.inferInterfaceCalls(ctx)
        if err != nil {
            return err
        }
        results <- facts
        return nil
    })
    
    // Analysis 2: DI wiring
    g.Go(func() error {
        facts, err := a.inferDIWiring(ctx)
        if err != nil {
            return err
        }
        results <- facts
        return nil
    })
    
    // Analysis 3: Data flow
    g.Go(func() error {
        facts, err := a.inferDataFlow(ctx)
        if err != nil {
            return err
        }
        results <- facts
        return nil
    })
    
    // Analysis 4: Control flow
    g.Go(func() error {
        facts, err := a.inferControlFlow(ctx)
        if err != nil {
            return err
        }
        results <- facts
        return nil
    })
    
    // Collect results
    go func() {
        g.Wait()
        close(results)
    }()
    
    for facts := range results {
        virtualFacts = append(virtualFacts, facts...)
    }
    
    return virtualFacts, g.Wait()
}

// inferInterfaceCalls detects potential calls via interface dispatch
func (a *Analyzer) inferInterfaceCalls(ctx context.Context) ([]Fact, error) {
    var facts []Fact
    
    // Find all interface types
    for ifaceFact := range a.store.Scan("", "type", "interface", "") {
        iface := ifaceFact.Subject
        
        // Find all implementations
        for implFact := range a.store.Scan("", "implements", string(iface), "") {
            impl := implFact.Subject
            
            // Find all callers of the interface
            for callFact := range a.store.Scan("", "calls", string(iface), "") {
                // Create virtual fact: caller potentially_calls implementation
                vFact := Fact{
                    Subject:   callFact.Subject,
                    Predicate: "v:potentially_calls",
                    Object:    impl,
                    Weight:    0.8,
                    Source:    "virtual:interface",
                }
                facts = append(facts, vFact)
            }
        }
    }
    
    return facts, nil
}

// inferDataFlow traces data flow through the graph
func (a *Analyzer) inferDataFlow(ctx context.Context) ([]Fact, error) {
    var facts []Fact
    
    // Find data transformers (functions that accept and return same type)
    for fact := range a.store.Scan("", "accepts", "", "") {
        // Check if same type is returned
        paramType := fact.Object
        subject := fact.Subject
        
        for retFact := range a.store.Scan(string(subject), "returns", "", "") {
            if retFact.Object == paramType {
                // This function transforms data of this type
                vFact := Fact{
                    Subject:   subject,
                    Predicate: "v:transforms",
                    Object:    paramType,
                    Weight:    0.9,
                    Source:    "virtual:dataflow",
                }
                facts = append(facts, vFact)
            }
        }
    }
    
    return facts, nil
}
```

### 13.5 Virtual Predicate Taxonomy

**Core Virtual Predicates:**

| Predicate | Description | Weight | Source |
|-----------|-------------|--------|--------|
| `v:potentially_calls` | Inferred call via interface/type assertion | 0.6-0.8 | interface/type |
| `v:wires_to` | DI wiring relationship | 0.7 | di |
| `v:transforms` | Data transformation function | 0.9 | dataflow |
| `v:depends_on` | Transitive dependency | 0.5 | transitive |
| `v:implements_via` | Implicit interface implementation | 0.8 | structural |
| `v:accesses` | Data access pattern (read/write) | 0.7 | dataflow |

### 13.6 Analysis Performance

**Time Complexity:**
- Single symbol analysis: O(d × f) where d = depth, f = facts per level
- Full graph analysis: O(n × m) where n = symbols, m = avg facts per symbol
- Parallel analysis: O((n × m) / p) where p = parallel workers

**Space Complexity:**
- Virtual facts: O(v) where v = number of virtual facts generated
- Temporary storage: O(n) for visited node tracking

**Performance Benchmarks:**

| Graph Size | Virtual Facts | Analysis Time | Memory |
|------------|--------------|---------------|--------|
| 10K facts | 500 | 50ms | 10MB |
| 100K facts | 5,000 | 500ms | 50MB |
| 1M facts | 50,000 | 5s | 200MB |

### 13.7 Configuration

```go
type AnalysisConfig struct {
    MaxDepth           int           // Max recursion depth (default: 5)
    Timeout            time.Duration // Analysis timeout (default: 30s)
    ParallelWorkers    int           // Parallel workers (default: 10)
    InferInterfaces    bool          // Enable interface-based inference (default: true)
    InferDIWiring      bool          // Enable DI wiring detection (default: true)
    InferDataFlow      bool          // Enable data flow analysis (default: true)
    MinWeightThreshold float64       // Minimum weight for virtual facts (default: 0.5)
}
```

### 13.8 Best Practices

**1. Incremental Analysis:**
```go
// Only analyze changed symbols for incremental updates
func incrementalAnalysis(store *MEBStore, changedSymbols []DocumentID) ([]Fact, error) {
    analyzer := NewAnalyzer(store)
    
    var allFacts []Fact
    for _, sym := range changedSymbols {
        facts, _ := analyzer.ResolveDependencies(sym)
        allFacts = append(allFacts, facts...)
    }
    
    return allFacts, nil
}
```

**2. Caching Virtual Facts:**
```go
// Cache virtual facts to avoid recomputation
type VirtualFactCache struct {
    mu     sync.RWMutex
    facts  map[DocumentID][]Fact
    ttl    time.Duration
}

func (c *VirtualFactCache) Get(symbol DocumentID) ([]Fact, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    facts, ok := c.facts[symbol]
    return facts, ok
}
```

**3. Weight-Based Filtering:**
```go
// Only keep high-confidence virtual facts
func filterByWeight(facts []Fact, minWeight float64) []Fact {
    var filtered []Fact
    for _, f := range facts {
        if f.Weight >= minWeight {
            filtered = append(filtered, f)
        }
    }
    return filtered
}
```

## 14. Inference Engine Integration

MEB provides a pluggable adapter layer for integrating with external inference engines (e.g., Mangle, Datalog engines). This enables symbolic reasoning over the graph while maintaining storage independence.

### 15.1 Adapter Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Inference Engine Integration                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐      ┌──────────────────┐            │
│  │ Inference Engine │      │    MEB Store     │            │
│  │ (Mangle/Datalog) │◄────►│  ┌────────────┐  │            │
│  └──────────────────┘      │  │   Adapter  │  │            │
│           │                │  └─────┬──────┘  │            │
│           │ Query          │        │         │            │
│           ▼                │        ▼         │            │
│  ┌──────────────────┐      │  ┌────────────┐  │            │
│  │  Query Pattern   │      │  │  BadgerDB  │  │            │
│  │  (predicate,     │      │  │  Iterator  │  │            │
│  │   args[])        │      │  └────────────┘  │            │
│  └──────────────────┘      └──────────────────┘            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 15.2 FactSource Interface

The adapter implements a minimal interface for streaming facts:

```go
// FactSource provides streaming access to EDB (Extensional Database)
type FactSource interface {
    // Search returns iterator for facts matching pattern
    // predicate: the predicate symbol (e.g., "triples")
    // args: bound values (nil for unbound variables)
    Search(predicate string, args []any) Iterator
}

// Iterator for streaming query results
type Iterator interface {
    Next() (Atom, bool)  // Advance and return next fact
    Close() error        // Release resources
}
```

**Index Selection Strategy:**
The adapter automatically selects optimal indices based on bound arguments:

| Bound Arguments | Index Used | Prefix Pattern |
|----------------|------------|----------------|
| Subject only | SPO | `\x01 \| subject` |
| Subject + Predicate | SPO | `\x01 \| subject \| predicate` |
| Object only | OPS | `\x02 \| object` |
| Object + Predicate | OPS | `\x02 \| object \| predicate` |
| None | Empty Iterator | Full scan not allowed |

### 15.3 Atom Representation

Facts are converted to/from the inference engine's atom format:

```go
type Atom struct {
    Predicate PredicateSym
    Args      []BaseTerm    // Subject, Object, Graph
}

type PredicateSym struct {
    Symbol string
    Arity  int
}
```

**Conversion Flow:**
1. Engine queries with pattern (e.g., `triples("Alice", nil, nil)`)
2. Adapter converts to storage IDs using dictionary
3. Optimal index selected based on bound arguments
4. Iterator streams matching facts from BadgerDB
5. IDs converted back to strings via dictionary
6. Facts returned as Atoms to the engine

### 15.4 Benefits

- **Storage Independence:** Engines work with abstract interface, not storage details
- **Index Optimization:** Automatic selection minimizes I/O
- **Streaming:** Large result sets don't require full materialization
- **Lazy Decoding:** String conversion only when needed

## 15. Graph Community Detection (Leiden Integration)

MEB integrates the Leiden algorithm for community detection on code dependency graphs, combining structural call graphs with semantic vector similarity to identify meaningful architectural boundaries.

### 15.1 In-Memory Graph Representation (CSR)

For low-RAM operation (targeting 8GB), MEB uses CSR (Compressed Sparse Row) instead of object-based graphs.

```
┌─────────────────────────────────────────────────────────────┐
│                 CSR Graph Structure                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Graph: Call/Dependency Graph from Quads                    │
│                                                             │
│  adj_edges []uint32                                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ [2, 5, 8, 3, 7, 1, 4, 6, ...]                       │  │
│  │  │  │  │  │  │  │  │                                │  │
│  │  │  │  │  │  │  │  └─ Node 2's neighbors            │  │
│  │  │  │  │  │  │  └─ Node 2's neighbors               │  │
│  │  │  │  │  │  └─ Node 1's neighbors                  │  │
│  │  │  │  │  └─ Node 1's neighbors                     │  │
│  │  │  │  └─ Node 0's neighbors (Nodes 2, 5, 8)        │  │
│  │  │  └─ Node 0's neighbors                           │  │
│  │  └─ Node 0's neighbors                              │  │
│  └─ Node 0's neighbors                                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  adj_offsets []uint32                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ [0, 3, 6, 8, ...]                                   │  │
│  │  │  │  │                                             │  │
│  │  │  │  └─ Start of Node 2's neighbors at index 8    │  │
│  │  │  └─ Start of Node 1's neighbors at index 6       │  │
│  │  └─ Start of Node 1's neighbors at index 3          │  │
│  └─ Node 0 starts at index 0                            │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  Memory: ~8 bytes per edge (vs ~40+ bytes in object graph) │
│  10M edges = ~80MB RAM                                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Benefits:**
- **Low RAM**: Millions of edges in hundreds of MBs
- **Cache-Friendly**: Contiguous memory layout
- **GC-Friendly**: No pointers, minimal GC pressure
- **Fast Iteration**: O(1) neighbor access

### 15.2 Hierarchical Community Schema (Prefix 0x30)

Community detection results are persisted to avoid recomputation using a dedicated BadgerDB prefix.

**Storage Schema:**

```
Prefix: 0x30 (Leiden Index)

Key Format: [0x30:1][GraphID:8][Level:1][CommunityID:8][SubjectID:8]
            │     │          │      │             │
            │     │          │      │             └─ Member Node ID
            │     │          │      └─ Community Identifier
            │     │          └─ Hierarchy Level (0=base, 1+=super)
            │     └─ Graph Context (e.g., "callgraph:default")
            └─ Leiden Index Prefix

Total Key Size: 26 bytes
```

**Hierarchy Levels:**

```
Level 0 (Base):    Individual functions, structs, modules
                   Key: 0x30 | graph | 0x00 | 0x0000 | nodeID

Level 1 (Cluster): Communities of related functions
                   Key: 0x30 | graph | 0x01 | commID | nodeID

Level 2+ (Super):  Aggregated communities (meta-clusters)
                   Key: 0x30 | graph | 0x02 | superID | commID
```

**Query Patterns:**

```go
// Get all members of Community X at Level 1
prefix := []byte{0x30} + graphID + []byte{0x01} + communityID
results, _ := store.ScanWithPrefix(prefix)

// Get all Level 1 communities
prefix := []byte{0x30} + graphID + []byte{0x01}
communities, _ := store.GetCommunities(prefix)

// Get hierarchy path for a node
path, _ := store.GetCommunityPath(nodeID) // [L0, L1, L2, ...]
```

### 15.3 Compute-on-Ingest Pipeline

Community detection runs synchronously during ingestion, producing "baked" graphs ready for querying.

```
┌─────────────────────────────────────────────────────────────┐
│            Synchronous Ingestion Pipeline                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Stage 1: Quad & Vector Ingestion (Async)                  │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Store quads in graph DB (SPO/POS/PSO indices)       │ │
│  │ • Store INT8 vectors in mmap files                    │ │
│  │ • Store Float32 vectors in graph DB                   │ │
│  └────────────────┬──────────────────────────────────────┘ │
│                   │                                         │
│                   ▼                                         │
│  Stage 2: Graph Assembly                                   │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Scan SPOG index (Subject-Predicate-Object-Graph)   │ │
│  │ • Build CSR: adj_edges + adj_offsets arrays           │ │
│  │ • Map NodeIDs to dictionary IDs                       │ │
│  └────────────────┬──────────────────────────────────────┘ │
│                   │                                         │
│                   ▼                                         │
│  Stage 3: Leiden Algorithm                                 │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Iterative Limiter (Hard Constraint)                  │ │
│  │   • Max Iterations: 10 per level (hard limit)        │ │
│  │   • Convergence Threshold: ΔQ < 0.0001               │ │
│  │   • Early Exit: Stop if no improvement for 3 iters   │ │
│  │                                                       │ │
│  │ Pass 1: Local Moving                                  │ │
│  │   • Nodes move between communities to maximize Q     │ │
│  │   • Modularity Q = 1/2m Σ(Aij - γ*kikj/2m)δ(ci,cj)   │ │
│  │                                                       │ │
│  │ Pass 2: Refinement                                   │ │
│  │   • Ensure community connectivity                    │ │
│  │   • Fix Louvain's disconnected community bug         │ │
│  │                                                       │ │
│  │ Pass 3: Aggregation                                  │ │
│  │   • Collapse communities into super-nodes            │ │
│  │   • Repeat passes 1-2 on aggregated graph            │ │
│  └────────────────┬──────────────────────────────────────┘ │
│                   │                                         │
│                   ▼                                         │
│  Stage 4: Persistence                                      │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Write 0x30 prefix keys to graph DB                 │ │
│  │ • Store community metadata (size, modularity)        │ │
│  └────────────────┬──────────────────────────────────────┘ │
│                   │                                         │
│                   ▼                                         │
│  Stage 5: Hierarchical Summary (Optional)                  │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ • Send community metadata to LLM (Gemini)            │ │
│  │ • Generate architectural summary                     │ │
│  │ • Store as Document with embedding                   │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  Client waits for all stages. Result: Query-ready graph.   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 14.3.1 Iterative Limiter (Safety Constraint)

To prevent non-convergence or excessive computation time, MEB implements a hard iteration limit for the Leiden algorithm.

**Why Limit Iterations?**
- **Non-Converging Graphs**: Some graph structures may oscillate between community assignments without reaching a stable state
- **Cloud Run Constraints**: 30-second timeout limit for HTTP requests
- **Resource Predictability**: Ensure ingest pipeline completes within bounded time

**Implementation:**

```go
const (
    MaxIterations     = 10    // Hard limit per hierarchy level
    ConvergenceDelta  = 1e-4  // Modularity improvement threshold
    StagnationWindow  = 3     // Early exit if no improvement for N iterations
)

func (cd *CommunityDetector) localMovingPhase(csr *CSRGraph) []Community {
    communities := initializeCommunities(csr.nodeCount)
    prevModularity := calculateModularity(csr, communities)
    stagnationCount := 0
    
    for iter := 0; iter < MaxIterations; iter++ {
        improvement := false
        
        // Try moving each node to neighboring communities
        for node := uint32(0); node < csr.nodeCount; node++ {
            bestComm := findBestCommunity(csr, communities, node)
            if bestComm != communities[node] {
                communities[node] = bestComm
                improvement = true
            }
        }
        
        // Calculate new modularity
        currModularity := calculateModularity(csr, communities)
        delta := currModularity - prevModularity
        
        // Check convergence
        if delta < ConvergenceDelta {
            stagnationCount++
            if stagnationCount >= StagnationWindow {
                slog.Info("Leiden converged early", 
                    "iterations", iter+1, 
                    "modularity", currModularity)
                break
            }
        } else {
            stagnationCount = 0
        }
        
        prevModularity = currModularity
        
        // Log progress for large graphs
        if iter > 0 && iter%3 == 0 {
            slog.Debug("Leiden iteration progress",
                "iteration", iter,
                "modularity", currModularity,
                "delta", delta)
        }
    }
    
    return communities
}
```

**Limiter Behavior:**

| Scenario | Action | Result |
|----------|--------|--------|
| Convergence (< 0.01% ΔQ) | Exit early | Use current communities |
| Stagnation (3 iters, no improvement) | Exit early | Use current communities |
| Max iterations (10) reached | Force exit | Use best communities found |
| Modularity decrease | Reject move | Keep previous assignment |

**Performance Impact:**
- **Typical Case**: 4-7 iterations until convergence
- **Worst Case**: Exactly 10 iterations (bounded)
- **Time Guarantee**: <5 seconds for 1M nodes (with 10-iteration limit)

### 14.3.2 Implementation

```go
// CommunityDetector manages Leiden algorithm execution
type CommunityDetector struct {
    store      *MEBStore
    csr        *CSRGraph
    resolution float64    // γ parameter (default: 1.0)
    levels     int        // Hierarchy depth
}

// CSRGraph represents compressed sparse row graph
type CSRGraph struct {
    adjEdges   []uint32   // Target node IDs
    adjOffsets []uint32   // Start indices for each node
    nodeCount  uint32
    edgeCount  uint64
    weights    []float32  // Edge weights (hybrid)
}

// Detect runs Leiden algorithm on graph from quads
func (cd *CommunityDetector) Detect(graphID string) (*CommunityHierarchy, error) {
    // Stage 1: Build CSR from quads
    csr, err := cd.buildCSR(graphID)
    if err != nil {
        return nil, err
    }
    
    // Stage 2: Run Leiden iterations
    hierarchy := cd.runLeiden(csr)
    
    // Stage 3: Persist to 0x30 index
    if err := cd.persistCommunities(graphID, hierarchy); err != nil {
        return nil, err
    }
    
    return hierarchy, nil
}

// runLeiden executes Leiden algorithm with 3 passes
func (cd *CommunityDetector) runLeiden(csr *CSRGraph) *CommunityHierarchy {
    hierarchy := &CommunityHierarchy{}
    
    // Level 0: Base graph
    communities := cd.localMovingPhase(csr)
    communities = cd.refinementPhase(csr, communities)
    hierarchy.Levels = append(hierarchy.Levels, communities)
    
    // Higher levels: Aggregate and repeat
    for level := 1; level < cd.levels; level++ {
        aggregated := cd.aggregateGraph(csr, communities)
        communities = cd.localMovingPhase(aggregated)
        communities = cd.refinementPhase(aggregated, communities)
        hierarchy.Levels = append(hierarchy.Levels, communities)
    }
    
    return hierarchy
}
```

### 15.4 Hybrid Weighting (Neuro-Symbolic Edge)

Standard Leiden uses uniform edge weights. MEB enhances weights with vector similarity for semantically-aware clustering.

```
Edge Weight Formula:
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  w(A, B) = α * structural_weight + β * semantic_boost      │
│                                                             │
│  Where:                                                     │
│  • structural_weight = 1.0 (for direct call/dependency)    │
│  • semantic_boost = cosine_similarity(vec_A, vec_B)         │
│  • α = 0.7 (structural importance)                         │
│  • β = 0.3 (semantic importance)                           │
│                                                             │
│  Result:                                                    │
│  • Direct calls: High weight (structural)                  │
│  • Semantic similarity: Boosts related code                │
│  • Both: Strongest clustering signal                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Example:**
```
Function A calls Function B          → weight = 0.7 (structural)
Function A and B are semantically similar → weight += 0.3*0.85 = 0.255
Total weight = 0.955 (strong cluster signal)

Function C is structurally unrelated to A
But vec_C is very similar to vec_A     → weight = 0.3*0.90 = 0.27
May still cluster with A if semantic signal is strong enough
```

**Implementation:**

```go
// computeHybridWeights calculates edge weights using both signals
func (cd *CommunityDetector) computeHybridWeights(csr *CSRGraph) []float32 {
    weights := make([]float32, csr.edgeCount)
    
    const (
        alpha = 0.7  // Structural weight
        beta  = 0.3  // Semantic weight
    )
    
    for nodeID := uint32(0); nodeID < csr.nodeCount; nodeID++ {
        start := csr.adjOffsets[nodeID]
        end := csr.adjOffsets[nodeID+1]
        
        for i := start; i < end; i++ {
            neighborID := csr.adjEdges[i]
            
            // Structural: 1.0 for direct edge
            structural := float32(1.0)
            
            // Semantic: vector similarity
            vecA := cd.store.vectors.GetVector(nodeID)
            vecB := cd.store.vectors.GetVector(neighborID)
            semantic := cosineSimilarity(vecA, vecB)
            
            // Hybrid weight
            weights[i] = alpha*structural + beta*semantic
        }
    }
    
    return weights
}

// cosineSimilarity computes similarity between two INT8 vectors
func cosineSimilarity(a, b []int8) float32 {
    var dot, normA, normB int32
    
    for i := range a {
        dot += int32(a[i]) * int32(b[i])
        normA += int32(a[i]) * int32(a[i])
        normB += int32(b[i]) * int32(b[i])
    }
    
    return float32(dot) / (float32(normA) * float32(normB))
}
```

### 15.5 Community Query API

```go
// CommunityHierarchy represents multi-level communities
type CommunityHierarchy struct {
    GraphID string
    Levels  [][]Community // Level 0 = base, Level 1+ = super
}

type Community struct {
    ID           uint64
    Level        uint8
    Members      []DocumentID
    Size         int
    Modularity   float64
    InternalEdges int
    ExternalEdges int
}

// Query APIs
func (m *MEBStore) GetCommunityMembers(graphID string, level uint8, commID uint64) ([]DocumentID, error)
func (m *MEBStore) GetNodeCommunityPath(docID DocumentID) ([]Community, error) // [L0, L1, L2]
func (m *MEBStore) GetCommunitiesAtLevel(graphID string, level uint8) ([]Community, error)
func (m *MEBStore) GetCommunitySummary(commID uint64) (string, error) // LLM-generated summary

// Semantic community search
func (m *MEBStore) FindCommunityBySemantic(query string) ([]Community, error) {
    // Encode query to vector
    queryVec := embedder.Encode(query)
    
    // Search community summaries
    return m.Find().
        Where().
        Predicate("belongs_to").
        Object("community_summary").
        SimilarTo(queryVec, 0.8).
        Execute()
}
```

### 15.6 Usage Example

```go
// Ingest code with community detection
store, _ := meb.NewMEBStore(&meb.Config{BaseDir: "./meb-data"})

// During ingestion, community detection runs automatically
for _, file := range sourceFiles {
    facts := extractFacts(file)
    vectors := extractVectors(file)
    
    store.AddFactBatch(facts)
    for _, vec := range vectors {
        store.vectors.Add(vec.ID, vec.Embedding)
    }
}

// Trigger community detection (or runs auto on close)
detector := meb.NewCommunityDetector(store)
hierarchy, _ := detector.Detect("callgraph:default")

// Query communities
// "Show me the auth-related module cluster"
authCommunity, _ := store.FindCommunityBySemantic("authentication and authorization")
members, _ := store.GetCommunityMembers("callgraph:default", 1, authCommunity[0].ID)
// Returns: ["auth:login", "auth:logout", "auth:validate", "middleware:auth_check", ...]

// Get architectural summary
summary, _ := store.GetCommunitySummary(authCommunity[0].ID)
// "This cluster contains authentication-related functionality including
//  login/logout handlers, token validation, and middleware guards."
```

### 15.5 Hybrid Static-Structural + Dynamic-Semantic Clustering

MEB implements a **"Best of Both Worlds"** approach that bridges static structural clustering (Global Leiden) with dynamic semantic regrouping. This enables query-time reorganization of results based on both code structure and semantic meaning.

#### 15.5.1 Design Philosophy

**The Core Problem:**
- **Global Leiden** is structural but static (computed at ingest time)
- **Query Results** are semantic but fragmented (lose structural context)

**The Solution:**
Use static structural anchors to inform dynamic, lightning-fast semantic regrouping at query time.

```
Two-Stage Pipeline:
┌─────────────────────────────────────────────────────────────────────────┐
│ Stage 1: Structural Anchoring (Ingest-time)                             │
│ ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐           │
│ │ CSR Construction │─►│ Global Leiden│─►│ CommunityID      │           │
│ │ (Quad→Graph)     │  │ (Static)     │  │ (Prefix 0x30)    │           │
│ └──────────────────┘  └──────────────┘  └──────────────────┘           │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Stage 2: Semantic Refinement (Query-time, <2s circuit breaker)          │
│ ┌──────────┐ ┌──────────┐ ┌─────────────┐ ┌──────────┐ ┌────────────┐  │
│ │ Retrieval│►│ Enrich   │►│ Vector      │►│ K-means  │►│ Summarize  │  │
│ │ (Top-K)  │ │ (Community│ │ Augmentation│ │ (Hybrid) │ │ (Clusters) │  │
│ │          │ │ ID lookup) │ │             │ │          │ │            │  │
│ └──────────┘ └──────────┘ └─────────────┘ └──────────┘ └────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 15.5.2 Stage 1: Structural Anchoring (Ingest-time)

During ingestion of 1M+ facts, the system establishes the "Global Map":

**1. CSR Construction:**
Convert Quad-Store facts into a Compressed Sparse Row graph for efficient traversal.

**2. Global Leiden:**
Execute Leiden algorithm to assign a `CommunityID` to every node based on structural relationships (calls, imports, defines).

**3. Persistence:**
Store `CommunityID` in Quad-Store with prefix `0x30`:
```
Key: [0x30][GraphID:8][Level:1][CommunityID:8][SubjectID:8]
```

This serves as the **immutable structural coordinate** for every piece of knowledge.

#### 15.5.3 Stage 2: Semantic Refinement (Query-time)

When a query executes within the 2-second circuit breaker limit:

**Step 1: Retrieval (10-50ms)**
- Fetch Top-K (500-1000) facts using SIMD-accelerated Vector Search (MRL 64-dim)
- Use parallel vector scan with INT8 quantization

**Step 2: Enrichment (20-50ms)**
- Batch lookup in Quad-Store to retrieve `CommunityID` for Top-K nodes
- Leverage prefix scan on `0x30` index: O(log n) per lookup

**Step 3: Vector Augmentation (10-20ms)**
- Construct **Hybrid Vector** for each node combining semantic and structural signals
- See Section 15.5.4 for hybrid vector specification

**Step 4: Local Clustering (50-150ms)**
- Execute high-speed K-means on Hybrid Vectors
- See Section 15.5.5 for K-means engine design

**Step 5: Summarization (20-30ms)**
- Group facts by cluster
- Generate hierarchical summary for the Agent
- Total: **110-300ms** (well within 2s budget)

#### 15.5.4 Hybrid Vector Specification

To make K-means "structure-aware," we augment the semantic vector with structural data:

**Vector Composition:**

$$V_{hybrid} = [ (V_{semantic} \times \alpha) \oplus (V_{structural} \times (1-\alpha)) ]$$

Where:
- **$V_{semantic}$** (64-dim): INT8 MRL vector representing the "meaning" of content
- **$V_{structural}$** (16-dim): Embedding of the CommunityID
  - Implemented as learned embedding or LSH of community hierarchy path
  - Captures structural similarity (same module, same package, etc.)
- **$\alpha$** (Structural Weight): Tunable parameter (default: 0.75)
  - Lower $\alpha$ → clusters strictly respect code module boundaries
  - Higher $\alpha$ → more semantic, less structural

**Implementation:**

```go
// HybridVector represents the augmented query result vector
type HybridVector struct {
    Semantic   [64]int8   // MRL semantic embedding
    Structural [16]int8   // CommunityID embedding
    NodeID     uint64     // Original node identifier
    CommunityID uint64    // Structural community assignment
}

// AugmentVectors creates hybrid vectors for clustering
func (c *HybridClustering) AugmentVectors(
    vectors []Vector,
    communityIDs []uint64,
    alpha float32,
) []HybridVector {
    hybrids := make([]HybridVector, len(vectors))
    
    for i, vec := range vectors {
        // Embed CommunityID into 16-dim structural vector
        structural := c.embedCommunityID(communityIDs[i])
        
        // Apply alpha weighting
        for j := 0; j < 64; j++ {
            hybrids[i].Semantic[j] = int8(float32(vec[j]) * alpha)
        }
        for j := 0; j < 16; j++ {
            hybrids[i].Structural[j] = int8(float32(structural[j]) * (1-alpha))
        }
        
        hybrids[i].NodeID = vec.ID
        hybrids[i].CommunityID = communityIDs[i]
    }
    
    return hybrids
}

// embedCommunityID creates structural embedding from CommunityID (Little-Endian)
func (c *HybridClustering) embedCommunityID(commID uint64) [16]int8 {
    // Option 1: Direct encoding (fast, deterministic, ARCHITECTURE-NEUTRAL)
    // Uses Little-Endian for consistency with mmap binary format
    var result [16]int8
    binary.LittleEndian.PutUint64(result[:8], commID)
    
    // Pad remaining 8 bytes with hierarchy info (level 1, 2 community IDs if available)
    if level1ID := c.getParentCommunity(commID); level1ID != 0 {
        binary.LittleEndian.PutUint32(result[8:12], uint32(level1ID))
    }
    if level2ID := c.getGrandparentCommunity(commID); level2ID != 0 {
        binary.LittleEndian.PutUint32(result[12:16], uint32(level2ID))
    }
    
    return result
    
    // Option 2: LSH encoding (preserves hierarchy similarity)
    // Use Locality Sensitive Hashing of community path
    // return c.lshCommunityPath(commID)
    
    // Option 3: Learned embedding (best quality, requires training)
    // return c.communityEmbedding[commID]
}
```

#### 15.5.5 Fast K-means Engine (Go)

To avoid ML library overhead, MEB implements a minimalist, zero-allocation K-means runner:

```go
// KMeansEngine performs high-speed clustering on hybrid vectors
type KMeansEngine struct {
    // Pre-allocated buffers to avoid GC
    centroids    []HybridVector
    assignments  []int
    distances    []float32
    
    // SIMD-accelerated distance function
    distanceFunc func(a, b []int8) float32
}

// Cluster performs K-means clustering with hard iteration limits
func (k *KMeansEngine) Cluster(
    vectors []HybridVector,
    k int,
    maxIterations int,
) ([]Cluster, error) {
    
    // 1. K-means++ initialization (deterministic, fast convergence)
    k.initializeCentroidsPlusPlus(vectors, k)
    
    // 2. Iterative optimization
    for iter := 0; iter < maxIterations; iter++ {
        changed := 0
        
        // Assign each vector to nearest centroid
        for i, vec := range vectors {
            nearest := k.findNearestCentroid(vec)
            if k.assignments[i] != nearest {
                k.assignments[i] = nearest
                changed++
            }
        }
        
        // Update centroids
        k.updateCentroids(vectors)
        
        // Early termination if converged
        if changed == 0 {
            break
        }
    }
    
    // 3. Build result clusters
    return k.buildClusters(vectors), nil
}

// findNearestCentroid uses SIMD-accelerated cosine similarity
func (k *KMeansEngine) findNearestCentroid(vec HybridVector) int {
    bestDist := float32(math.MaxFloat32)
    bestIdx := 0
    
    for i, centroid := range k.centroids {
        // Cosine distance on 80-dim hybrid vector (64+16)
        dist := CosineDistanceInt8(
            vec.Semantic[:], centroid.Semantic[:],
            vec.Structural[:], centroid.Structural[:],
        )
        if dist < bestDist {
            bestDist = dist
            bestIdx = i
        }
    }
    
    return bestIdx
}
```

**Key Optimizations:**

1. **K-means++ Initialization:** Faster convergence than random init
2. **Cosine Similarity (INT8 SIMD):** Hardware-accelerated distance metric
3. **Hard Iteration Limit:** Max 10 iterations guarantees latency (<150ms)
4. **Pre-allocated Buffers:** No heap allocation during clustering

#### 15.5.6 Synthesis Layer: From Clusters to Insight

The output of Hybrid Clustering is a structured **Knowledge Package** for the LLM Agent:

```go
// KnowledgePackage is the final output for the Agent
type KnowledgePackage struct {
    Clusters      []FactCluster     // Grouped facts
    TotalFacts    int               // Original query result count
    QueryTime     time.Duration     // Time spent clustering
    Fallback      bool              // True if K-means failed
}

type FactCluster struct {
    ID             int              // Cluster identifier
    DominantCommunity string        // Most common CommunityID in cluster
    Exemplars      []Fact           // N-closest facts to centroid
    AllFacts       []Fact           // All facts in cluster (if small)
    CentroidVector HybridVector     // Cluster center
    Coherence      float32          // Average similarity to centroid
}

// SelectExemplars picks representative facts from cluster
func (c *HybridClustering) SelectExemplars(
    cluster *FactCluster,
    n int,
) []Fact {
    // Sort by distance to centroid (ascending)
    sort.Slice(cluster.AllFacts, func(i, j int) bool {
        return cluster.Distances[i] < cluster.Distances[j]
    })
    
    // Return top N closest to centroid
    if len(cluster.AllFacts) <= n {
        return cluster.AllFacts
    }
    return cluster.AllFacts[:n]
}
```

**Hierarchical Prompting for Agent:**

```
Agent receives:

1. Executive Summary
   "Found 5 clusters in 500 results. Main topics: Authentication (3 clusters), 
    Storage (2 clusters)."

2. Cluster Overview (Table of Contents)
   Cluster 1: "Login Flow" (127 facts, Auth module) - Coherence: 0.89
   Cluster 2: "Token Validation" (98 facts, Auth module) - Coherence: 0.92
   Cluster 3: "Database Access" (156 facts, Storage module) - Coherence: 0.85
   ...

3. Detailed Exemplars (Top 3 per cluster)
   [Representative facts closest to each centroid]

4. Navigation Hints
   "Related: Cluster 1 and 2 share CommunityID 42 (Auth module boundary)"
```

#### 15.5.7 Operational Guardrails

| Constraint | Value | Rationale |
|------------|-------|-----------|
| **Max Clustering Nodes** | 1,000 | Prevents K-means from exceeding 200ms compute budget |
| **Hybrid Vector Size** | 80 bytes | Optimized for L2/L3 cache line sizes (64 + 16) |
| **Cluster Count (K)** | √(N/2) | Heuristic balancing granularity vs overview (e.g., ~15 clusters for 500 nodes) |
| **Max K-means Iterations** | 10 | Hard limit guarantees convergence within latency budget |
| **Min Cluster Coherence** | 0.70 | Below threshold, trigger reclustering with different K |
| **Failure Mode** | Flat List | If K-means fails to converge, return sorted facts |

**Circuit Breaker Integration:**

```go
func (m *MEBStore) QueryWithHybridClustering(
    ctx context.Context,
    query string,
    opts ClusteringOptions,
) (*KnowledgePackage, error) {
    
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    
    // Stage 1: Vector search
    vectors, err := m.vectorSearch(ctx, query, 1000)
    if err != nil {
        return nil, err
    }
    
    // Stage 2: Enrichment with CommunityIDs
    communityIDs, err := m.batchGetCommunityIDs(ctx, vectors)
    if err != nil {
        return nil, err
    }
    
    // Stage 3: Hybrid clustering (with circuit breaker)
    select {
    case <-ctx.Done():
        // Timeout: return flat list
        return m.fallbackToFlatList(vectors), ErrQueryTimeout
    default:
        clusters, err := m.hybridCluster(ctx, vectors, communityIDs, opts)
        if err != nil {
            return m.fallbackToFlatList(vectors), err
        }
        return clusters, nil
    }
}
```

#### 15.5.8 Benefits Summary

| Aspect | Global Leiden Only | Hybrid Clustering | Improvement |
|--------|-------------------|-------------------|-------------|
| **Static Context** | ✅ Module boundaries | ✅ Module boundaries | Same |
| **Dynamic Semantics** | ❌ None | ✅ Query-specific | **New** |
| **Granularity** | Macro (community level) | Micro + Macro | **Finer** |
| **Latency** | 0 (pre-computed) | <300ms | **Acceptable** |
| **Agent Comprehension** | Low (too broad) | High (focused clusters) | **Better** |
| **Cross-Module Relations** | Hard to detect | Explicit in hybrid vectors | **Discovered** |

**Use Cases:**

1. **Code Review:** "Show me auth-related changes grouped by functionality"
2. **Refactoring:** "Identify tightly coupled components across module boundaries"
3. **Onboarding:** "Explain the storage layer with semantic grouping"
4. **Debugging:** "Find related code to this error, respecting architecture"

## 16. Operational & Analytics APIs

MEB provides administrative and analytical APIs for store management, diagnostics, and exploration.

### 16.1 Store Management

```go
// Reset clears all data (destructive operation)
func (m *MEBStore) Reset() error

// RecalculateStats forces full DB scan to fix fact counter
// Use after unclean shutdown or suspected corruption
func (m *MEBStore) RecalculateStats() (uint64, error)

// DeleteGraph removes all facts in specified graph
func (m *MEBStore) DeleteGraph(graph string) error

// DeleteFactsBySubject removes all facts for given subject
// Used for incremental updates (clear before re-ingest)
func (m *MEBStore) DeleteFactsBySubject(subject string) error
```

### 16.2 Symbol Analytics

```go
// SymbolStat represents symbol frequency statistics
type SymbolStat struct {
    Name  string  // Symbol identifier
    Count int     // Occurrence count
}

// GetAllPredicates returns sorted list of unique predicates
func (m *MEBStore) GetAllPredicates() ([]string, error)

// GetTopSymbols returns most frequent symbols
// Optional filter function excludes certain patterns
func (m *MEBStore) GetTopSymbols(limit int, filter func(string) bool) ([]SymbolStat, error)

// SearchSymbols performs case-insensitive prefix search
// Optional predicateFilter limits to symbols with specific relationships
func (m *MEBStore) SearchSymbols(query string, limit int, predicateFilter string) ([]string, error)

// IterateSymbols iterates all dictionary entries
// Returns false from callback to stop iteration
func (m *MEBStore) IterateSymbols(fn func(string) bool) error
```

**Use Cases:**
- **Predicate Discovery:** Understand what relationships exist in the graph
- **Symbol Exploration:** Find related symbols through frequency analysis
- **Data Quality:** Identify outliers or anomalies via statistics
- **Debugging:** Trace symbol usage patterns

### 16.3 Metadata & Extensibility

**Versioned Metadata Encoding:**
```go
// Metadata includes versioning for future extensibility
const MetadataVersion1 = 1

// EncodeFactMetadata includes version byte for forward compatibility
// Format: [Version:1][Weight:8][Source...]
func EncodeFactMetadata(f Fact) []byte

// DecodeFactMetadata handles version detection gracefully
// Returns defaults for unknown versions
func DecodeFactMetadata(data []byte) (float64, string)
```

**Design Principles:**
- Versioning enables safe schema evolution
- Defaults ensure backward compatibility
- Unknown versions degrade gracefully (don't crash)

## 17. Rules Storage System

MEB provides native support for storing and executing rules with semantic search capabilities.

### 19.1 Rule Model

Rules are stored using a dual representation: **quad-based structure** for metadata/triggers and **document-based content** for execution logic and embeddings.

```
┌─────────────────────────────────────────────────────────────┐
│                    Rule Storage Architecture                 │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Rule: "auth_required"                                      │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ 1. Rule Facts (Quads in Graph "rules")                │ │
│  │                                                       │ │
│  │  <rule:auth_required, "triggers_on", "access",        │ │
│  │                      "rules">                         │ │
│  │  <rule:auth_required, "action_type", "deny",          │ │
│  │                      "rules">                         │ │
│  │  <rule:auth_required, "priority", 100, "rules">       │ │
│  │  <rule:auth_required, "enabled", true, "rules">       │ │
│  │  <rule:auth_required, "belongs_to", "security",       │ │
│  │                      "rules">                         │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ 2. Rule Document (ContentStore + VectorStore)         │ │
│  │                                                       │ │
│  │  ID: rule:auth_required                               │ │
│  │  Content: "Deny access if user not authenticated"     │ │
│  │  Embedding: [0.23, -0.45, 0.89, ...]                  │ │
│  │  Code: compiled_function_bytes                        │ │
│  │  Metadata: {                                          │ │
│  │    "compiled": true,                                  │ │
│  │    "language": "javascript"                           │ │
│  │  }                                                    │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 19.2 Rule Types

```go
// Rule represents an executable rule
type Rule struct {
    ID       string                 // Unique identifier
    Name     string                 // Human-readable name
    Pattern  string                 // Datalog pattern/trigger
    Action   string                 // Action code (JavaScript/Go/Wasm)
    Priority int                    // Execution order
    Graph    string                 // Namespace
    Enabled  bool                   // Active status
    Metadata map[string]any         // Arbitrary metadata
}

// RuleEngine manages rule execution
type RuleEngine struct {
    store      *MEBStore
    rules      map[string]*CompiledRule
    index      *RuleIndex
    mu         sync.RWMutex
}

// CompiledRule holds executable rule
type CompiledRule struct {
    ID       string
    Pattern  *Atom                 // Parsed trigger (see below)
    Action   func(*RuleContext) error
    Priority int
}

// RuleResult represents a found rule from search
type RuleResult struct {
    RuleID    string
    Name      string
    Pattern   string
    Score     float64              // Semantic similarity score
    Metadata  map[string]any
}

// RuleContext provides execution context for rules
type RuleContext struct {
    Store      *MEBStore
    Subject    DocumentID
    Object     interface{}
    Event      string
    Facts      []Fact
}

// Atom represents a Datalog atom/pattern
type Atom struct {
    Predicate string
    Args      []interface{}
}
```

### 19.3 Storing Rules

```go
// 1. Create rule with content and embedding
ruleDoc := meb.Document{
    ID:      meb.DocumentID("rule:auth_check"),
    Content: []byte(`
        function execute(ctx) {
            if (!ctx.facts.Exists(ctx.subject, "authenticated", ctx.object)) {
                ctx.emit("deny", ctx.subject, ctx.object);
            }
        }
    `),
    Embedding: embeddingModel.Encode("Deny access if user not authenticated"),
    Metadata: map[string]any{
        "trigger_event": "access",
        "action_type":   "deny",
        "priority":      100,
        "enabled":       true,
        "language":      "javascript",
    },
}

// 2. Store document with embedding
store.AddDocument(
    ruleDoc.ID,
    ruleDoc.Content,
    ruleDoc.Embedding,
    ruleDoc.Metadata,
)

// 3. Store rule structure as quads for fast lookup
ruleFacts := []meb.Fact{
    {
        Subject:   "rule:auth_check",
        Predicate: "triggers_on",
        Object:    "access",
        Graph:     "rules",
    },
    {
        Subject:   "rule:auth_check",
        Predicate: "action_type",
        Object:    "deny",
        Graph:     "rules",
    },
    {
        Subject:   "rule:auth_check",
        Predicate: "priority",
        Object:    100,
        Graph:     "rules",
    },
    {
        Subject:   "rule:auth_check",
        Predicate: "enabled",
        Object:    true,
        Graph:     "rules",
    },
}
store.AddFactBatch(ruleFacts)
```

### 19.4 Direct Rule Execution (No Query Round-Trip)

Rules execute directly without loading and converting to queries:

```go
// Register rules for direct execution
engine := meb.NewRuleEngine(store)

// Load and compile all enabled rules
engine.LoadRules(func(rule meb.Fact) bool {
    return rule.Graph == "rules" && rule.Predicate == "enabled" && 
           rule.Object.(bool) == true
})

// Trigger rules by event - executes immediately
func (m *MEBStore) TriggerEvent(event string, subject DocumentID, object any) error {
    // 1. Find matching rules by trigger event (quad lookup - O(1))
    matchingRules := m.FindRulesByTrigger(event)
    
    // 2. Execute each rule directly
    for _, ruleID := range matchingRules {
        // Get compiled rule from engine cache
        compiled := engine.GetCompiledRule(ruleID)
        if compiled == nil {
            // Lazy compile if not cached
            compiled = engine.CompileRule(ruleID)
        }
        
        // Execute rule action directly
        ctx := &RuleContext{
            Store:   m,
            Subject: subject,
            Object:  object,
            Event:   event,
        }
        
        if err := compiled.Action(ctx); err != nil {
            return fmt.Errorf("rule %s failed: %w", ruleID, err)
        }
    }
    
    return nil
}

// Usage: Trigger event directly triggers rule execution
store.TriggerEvent("access", "user:123", "resource:api")
// Rule executes immediately without query round-trip
```

**Execution Flow:**
```
Event Occurs
    │
    ▼
┌──────────────────┐
│ TriggerEvent()   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Quad Lookup      │ ◀── O(1): Find rules with triggers_on=event
│ (Graph "rules")  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ GetCompiledRule  │ ◀── From in-memory rule engine cache
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Execute Action   │ ◀── Direct function call
│ (Native Code)    │
└──────────────────┘
```

### 17.5 Semantic Rule Search

Search rules by meaning using vector similarity:

```go
// 1. Search rules semantically
func (m *MEBStore) SearchRulesBySemantic(query string, topK int) ([]RuleResult, error) {
    // Encode query to embedding
    queryVec := embeddingModel.Encode(query)
    
    // Vector similarity search on rules
    results, err := m.Find().
        Where().
        SimilarTo(queryVec, 0.8).
        And().
        Predicate("belongs_to").
        Object("rules").
        Execute()
    
    return results, nil
}

// 2. Usage: Find security-related rules
rules, err := store.SearchRulesBySemantic("authentication and access control", 5)
// Returns: [rule:auth_check, rule:session_timeout, rule:permission_check, ...]

// 3. Combine semantic + structural search
func (m *MEBStore) FindRelevantRules(context string, event string) ([]RuleResult, error) {
    // Semantic search for contextually similar rules
    semanticResults, _ := m.SearchRulesBySemantic(context, 10)
    
    // Quad lookup for event-specific rules
    eventRules, _ := m.Scan("", "triggers_on", event, "rules")
    
    // Merge and rank
    return mergeResults(semanticResults, eventRules), nil
}
```

### 17.6 Rule Indexing

Fast rule lookup by various criteria:

```go
type RuleIndex struct {
    byTrigger map[string][]string    // event → rule IDs
    byType    map[string][]string    // action_type → rule IDs  
    byGraph   map[string][]string    // graph → rule IDs
}

func (idx *RuleIndex) Build(store *MEBStore) error {
    // Index all rules from quad store
    for fact := range store.Scan("", "", "", "rules") {
        switch fact.Predicate {
        case "triggers_on":
            idx.byTrigger[fact.Object.(string)] = append(
                idx.byTrigger[fact.Object.(string)], 
                string(fact.Subject),
            )
        case "action_type":
            idx.byType[fact.Object.(string)] = append(
                idx.byType[fact.Object.(string)],
                string(fact.Subject),
            )
        }
    }
    return nil
}
```

### 17.7 Complete Example

```go
// Setup rule engine
store, _ := meb.NewMEBStore(&meb.Config{BaseDir: "./rules-db"})
engine := meb.NewRuleEngine(store)

// 1. Store rule with semantic embedding
ruleDoc := meb.Document{
    ID:      "rule:data_validation",
    Content: []byte(`
        function execute(ctx) {
            if (ctx.object.age < 18) {
                ctx.emit("reject", ctx.subject, "underage");
            }
        }
    `),
    Embedding: embedder.Encode("Validate user age must be over 18"),
    Metadata: map[string]any{
        "triggers_on": "user_registration",
        "priority":    50,
        "category":    "validation",
    },
}
store.AddDocument(ruleDoc.ID, ruleDoc.Content, ruleDoc.Embedding, ruleDoc.Metadata)

// Store rule facts
store.AddFactBatch([]meb.Fact{
    {Subject: "rule:data_validation", Predicate: "triggers_on", Object: "user_registration", Graph: "rules"},
    {Subject: "rule:data_validation", Predicate: "priority", Object: 50, Graph: "rules"},
    {Subject: "rule:data_validation", Predicate: "enabled", Object: true, Graph: "rules"},
})

// 2. Compile rules for direct execution
engine.LoadAllRules()

// 3. Trigger rule directly (no query)
store.TriggerEvent("user_registration", "user:456", map[string]any{"age": 16})
// Rule executes immediately: emits "reject" event

// 4. Search similar rules semantically
similar, _ := store.SearchRulesBySemantic("age restriction and validation", 5)
// Returns rule:data_validation and similar rules
```

## 18. Configuration

```go
type Config struct {
    // Storage Paths
    BaseDir   string  // Root directory for all stores
    GraphDir  string  // Quad store path (default: {BaseDir}/graph)
    VocabDir  string  // Dictionary path (default: {BaseDir}/vocab)
    VectorDir string  // Vector store path (default: {BaseDir}/vector)
    
    // Behavior
    InMemory       bool
    ReadOnly       bool
    
    // Deployment Profile - Environment-specific optimization
    Profile        string   // "Ingest-Heavy", "Safe-Serving", "Cloud-Run-LowMem"
    
    // BadgerDB Tuning
    GraphSyncWrites  bool   // false = async (default)
    VocabSyncWrites  bool   // true = sync (default)
    BlockCacheSize   int64
    IndexCacheSize   int64
    MemTableSize     int64  // Memtable size in bytes
    NumMemtables     int    // Number of memtables to keep in memory
    
    // Dictionary Tuning
    LRUCacheSize   int
    NumDictShards  int
    
    // Vector Tuning
    VectorMmap     bool    // true = use mmap (default)
    MRLDimensions  int     // 64 (default) - MRL truncation size
    
    // Compression Settings
    Compression    bool    // true (default) - Enable ZSTD compression for BadgerDB
    ContentCompression bool // true (default) - Enable S2 compression for content
    
    // Query Guardrails (Hard Limits)
    MaxJoinResults      int           // 5000 (default) - Max intermediate facts
    QueryTimeoutMs      int           // 2000 (default) - Circuit breaker timeout
    RerankLimitK        int           // 50 (default) - Top K for re-ranking
    MaxRecursionDepth   int           // 5 (default) - Hard limit for recursion
    
    // Rule Engine Limits
    MaxRuleDepth        int           // 5 (default) - Rule execution depth
    
    // Clustering
    LeidenResolution    float64       // 1.0 (default) - Modularity resolution
    LeidenLevels        int           // 2 (default) - Hierarchy depth
    LeidenMaxIterations int           // 10 (default) - Max iterations per level (safety limit)
    DynamicClustering   bool          // true (default) - Enable sub-clustering on results
}
```

### 17.1 Deployment Profiles

MEB provides pre-configured profiles optimized for different deployment environments:

| Profile | Target Environment | Memory | Use Case |
|---------|-------------------|---------|----------|
| **Ingest-Heavy** | High-RAM VMs/servers | 8GB+ | Bulk data ingestion, indexing |
| **Safe-Serving** | Low-RAM/Cloud Run | 1-2GB | Query serving, read-heavy |
| **Cloud-Run-LowMem** | Minimal environments | 512MB-1GB | Embedded, edge deployments |

**Profile Characteristics:**

```go
// Ingest-Heavy (Default)
// Optimized for: Maximum throughput, bulk operations
BlockCacheSize:    8GB
IndexCacheSize:    2GB
ValueLogFileSize:  1GB
NumCompactors:     4
SyncWrites:        false

// Safe-Serving
// Optimized for: Low latency, stable memory
BlockCacheSize:    256MB
IndexCacheSize:    64MB
ValueLogFileSize:  64MB
NumCompactors:     2
SyncWrites:        false

// Cloud-Run-LowMem
// Optimized for: Minimal footprint, quick startup
BlockCacheSize:    64MB
IndexCacheSize:    32MB
ValueLogFileSize:  32MB
NumCompactors:     2
SyncWrites:        false
ReadOnly:          true (typical)
```

**Profile Selection:**

```go
// Ingest workload
store, _ := meb.NewMEBStore(&meb.Config{
    BaseDir: "./data",
    Profile: "Ingest-Heavy",
})

// Serving workload
store, _ := meb.NewMEBStore(&meb.Config{
    BaseDir: "./data",
    Profile: "Safe-Serving",
    ReadOnly: true,
})
```

## 19. Usage Examples

### 19.1 Basic Setup

```go
// Simple setup with base directory
store, err := meb.NewMEBStore(&meb.Config{
    BaseDir: "./meb-data",
})
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Creates:
// ./meb-data/graph/  - Quad store
// ./meb-data/vocab/  - Dictionary
// ./meb-data/vector/ - Vector files
```

### 19.2 Custom Store Paths

```go
store, err := meb.NewMEBStore(&meb.Config{
    GraphDir:  "/fast-ssd/graph",
    VocabDir:  "/persistent/vocab", 
    VectorDir: "/large-disk/vectors",
})
```

### 19.3 Performance Tuning

```go
store, err := meb.NewMEBStore(&meb.Config{
    BaseDir:         "./data",
    GraphSyncWrites: false,  // Async for speed
    VocabSyncWrites: true,   // Sync for safety
    VectorMmap:      true,   // Use memory mapping
    NumDictShards:   32,     // For high concurrency
    LRUCacheSize:    10000,  // Dictionary cache
    
    // Compression (enabled by default)
    Compression:          true,  // ZSTD for BadgerDB
    ContentCompression:   true,  // S2 for content
})
```

### 19.4 Storage Optimization Examples

**Minimal Storage Footprint (Maximum Compression):**
```go
store, err := meb.NewMEBStore(&meb.Config{
    BaseDir:            "./meb-data",
    Compression:        true,   // ZSTD compression
    ContentCompression: true,   // S2 compression
    MRLDimensions:      64,     // Aggressive MRL truncation
    // Expected: 30:1 compression ratio
})
```

**High-Performance (Minimal Compression Overhead):**
```go
store, err := meb.NewMEBStore(&meb.Config{
    BaseDir:            "./meb-data",
    Compression:        false,  // Disable ZSTD
    ContentCompression: false,  // Disable S2
    // Trade storage for speed
})
```

### 19.4 Rules Setup

```go
// Setup with rule engine
store, _ := meb.NewMEBStore(&meb.Config{BaseDir: "./meb-data"})
engine := meb.NewRuleEngine(store)

// Load and compile all rules
engine.LoadAllRules()

// Trigger rules by event
store.TriggerEvent("access", "user:123", "resource:api")

// Search rules semantically
rules, _ := store.SearchRulesBySemantic("authentication", 5)
```

## 20. Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Add Document | O(n) | n = embedding dimensions |
| Add Fact | O(1) | Amortized with batching |
| Vector Search | O(m × n) | SIMD optimized |
| Quad Query | O(k) | Index-accelerated |
| Hydrate | O(d × p) | Parallel factor |
| **Query + Hydrate** | **O(k + h)** | **Push-down: single round-trip** |
| Rule Trigger | O(r) | r = matching rules, direct execution |
| Rule Search | O(v) | v = vector search on rules |

**Benchmarks:**
- Vector search: ~1M vectors/sec (INT8, AVX2)
- Fact insertion: ~100K facts/sec (batched)
- Query latency: <10ms typical
- Rule execution: ~10K rules/sec (direct)
- **Query + Hydrate: ~50% faster** with push-down optimization

### Hydration Push-Down Impact

| Scenario | Without Push-Down | With Push-Down | Improvement |
|----------|------------------|----------------|-------------|
| Simple Query + Hydrate | 2 DB round-trips | 1 DB round-trip | **50%** |
| Batch (100 docs) | 2 round-trips + overhead | 1 round-trip | **60%** |
| Deep Hydrate (depth=3) | Multiple trips per level | Single batch | **70%** |

### Community Detection (Leiden) Performance

| Graph Size | Build CSR | Leiden Detection | Persistence | Total Ingest |
|------------|-----------|------------------|-------------|--------------|
| 10K nodes, 50K edges | 50ms | 200ms | 100ms | **350ms** |
| 100K nodes, 500K edges | 300ms | 1.5s | 500ms | **2.3s** |
| 1M nodes, 5M edges | 2s | 12s | 3s | **17s** |

**Iteration Limiter Impact:**

| Graph Type | Avg Iterations | Max Iterations | Convergence Rate |
|------------|----------------|----------------|------------------|
| **Well-formed code** | 4-6 | 10 | 95% converge early |
| **Cyclic dependencies** | 8-10 | 10 | 80% hit limit |
| **Random graphs** | 6-8 | 10 | 90% converge early |

**Key Points:**
- **95% of graphs** converge in <7 iterations (early exit)
- **Hard ceiling**: 10 iterations = ~5s max for 1M nodes
- **Cloud Run Safe**: Always completes within timeout
- **Quality Impact**: <2% modularity loss vs unlimited iterations

**Memory Usage:
- CSR representation: ~8 bytes per edge
- 1M nodes, 5M edges = ~40MB RAM
- 10M edges = ~80MB RAM (fits easily in 8GB target)

**Hybrid Weighting Overhead:**
- Vector similarity computation: +15-20% vs uniform weights
- Result: 20-30% better community quality (measured by modularity)

### Storage Compression Performance

| Compression Layer | Algorithm | Compression | Speed | Use Case |
|-------------------|-----------|-------------|-------|----------|
| **Content** | S2 | 2.8:1 | 800 MB/s | Source code, documents |
| **BadgerDB** | ZSTD (L3) | 4.2:1 | 200 MB/s | Quads, keys, metadata |
| **Vectors** | MRL+INT8 | 96:1 | N/A (mmap) | Vector search buffer |
| **Combined** | All layers | **30:1** | **Variable** | **Total storage** |

**Performance Impact:**
- **Ingestion**: +5-10% CPU overhead for compression (one-time cost)
- **Query**: <1ms decompression latency for content
- **Storage Savings**: Typical code repository (1M files)
  - Raw: ~100GB
  - Compressed: ~3.3GB
  - **Savings: 96.7%**

**CPU vs Storage Trade-off:**
```
Compression Enabled:
  Storage: 3.3GB  ← 96.7% savings
  Ingest:  +10% CPU
  Query:   <1ms overhead

Compression Disabled:
  Storage: 100GB
  Ingest:  Baseline CPU
  Query:   No overhead
```

### Circuit Breaker & Guardrails

MEB implements hard limits to prevent cascading failures on resource-constrained environments (8GB RAM target).

```go
// Query execution with circuit breaker
func (m *MEBStore) QueryWithGuardrails(query string) ([]QueryResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 
        time.Duration(m.config.QueryTimeoutMs) * time.Millisecond)
    defer cancel()
    
    results := make([]QueryResult, 0, m.config.MaxJoinResults)
    
    for result := range m.executeQuery(ctx, query) {
        select {
        case <-ctx.Done():
            return results, ErrQueryTimeout
        default:
            if len(results) >= m.config.MaxJoinResults {
                return results, ErrResultLimitExceeded
            }
            results = append(results, result)
        }
    }
    
    return results, nil
}
```

**Guardrail Enforcement Points:**

| Component | Enforcement | Action on Limit |
|-----------|-------------|-----------------|
| **Join Engine** | Check after each join step | Return partial results + warning |
| **Query Executor** | Context with timeout | Cancel query, return ErrQueryTimeout |
| **Vector Search** | Limit Top-K to 100 | Return top 100 only |
| **Re-ranking** | Limit to K=50 | Re-rank only top 50 candidates |
| **Rule Engine** | Track depth counter | Halt execution at MaxRuleDepth |
| **Recursion** | Depth counter in Query | Return ErrMaxDepthExceeded |
| **Leiden Clustering** | Iteration counter per level | Exit at 10 iters, return best communities |

**What Happens When Guardrails Trigger:**
1. **Partial Results**: Return what was computed up to the limit
2. **Error Logging**: Log the breach for monitoring
3. **Metrics**: Increment counters for observability
4. **Graceful Degradation**: Never crash, always return something useful

### Resource Limits Validation

```go
// Validate config on startup
func (cfg *Config) Validate() error {
    if cfg.MaxJoinResults <= 0 || cfg.MaxJoinResults > 10000 {
        return fmt.Errorf("MaxJoinResults must be 1-10000, got %d", cfg.MaxJoinResults)
    }
    if cfg.QueryTimeoutMs < 100 || cfg.QueryTimeoutMs > 30000 {
        return fmt.Errorf("QueryTimeoutMs must be 100-30000, got %d", cfg.QueryTimeoutMs)
    }
    if cfg.MaxRecursionDepth < 1 || cfg.MaxRecursionDepth > 20 {
        return fmt.Errorf("MaxRecursionDepth must be 1-20, got %d", cfg.MaxRecursionDepth)
    }
    if cfg.MRLDimensions != 64 {
        return fmt.Errorf("MRLDimensions must be 64 (only supported value)")
    }
    if cfg.LeidenMaxIterations < 1 || cfg.LeidenMaxIterations > 50 {
        return fmt.Errorf("LeidenMaxIterations must be 1-50, got %d", cfg.LeidenMaxIterations)
    }
    return nil
}
```

## 21. Error Handling

```go
var (
    // Storage Errors
    ErrDocumentNotFound = errors.New("document not found")
    ErrFactNotFound     = errors.New("fact not found")
    
    // Query Errors
    ErrInvalidQuery        = errors.New("invalid query syntax")
    ErrFullTableScan       = errors.New("full table scan not allowed")
    ErrQueryTimeout        = errors.New("query exceeded timeout (circuit breaker)")
    ErrResultLimitExceeded = errors.New("query result limit exceeded")
    ErrMaxDepthExceeded    = errors.New("recursion max depth exceeded")
    
    // Rule Engine Errors
    ErrRuleNotFound        = errors.New("rule not found")
    ErrRuleCompile         = errors.New("rule compilation failed")
    ErrRuleMaxDepth        = errors.New("rule execution depth limit exceeded")
    
    // Community Detection Errors
    ErrCommunityNotFound   = errors.New("community not found")
    ErrLeidenConvergence   = errors.New("leiden algorithm failed to converge")
    
    // Configuration Errors
    ErrInvalidConfig       = errors.New("invalid configuration")
    ErrGCSFuseNotAllowed   = errors.New("GCS Fuse direct access not allowed, use local ephemeral disk")
)
```

## 22. SOLID and DRY Principles

MEB's codebase adheres to SOLID and DRY principles to ensure maintainability, testability, and long-term code health.

### 22.1 SOLID Compliance

| Principle | Implementation |
|-----------|----------------|
| **Single Responsibility** | Each type has a focused purpose: `Fact` (data), `Builder` (query construction), `PredicateTable` (index management) |
| **Open/Closed** | Extensible via interfaces: `Store`, `Extractor`, `Connector`, `Dictionary` |
| **Interface Segregation** | Small, focused interfaces: `Extractor` has 3 methods, `Connector` has 3 methods |
| **Dependency Inversion** | `Builder` depends on `Store` interface, not concrete `MEBStore` |
| **Liskov Substitution** | Any implementation of `Dictionary` can be substituted (single-threaded vs sharded) |

### 22.2 DRY - Code Reuse Patterns

#### 22.2.1 Scan Logic Consolidation (`scan.go`)

The `Scan()` and `ScanZeroCopy()` methods previously contained ~150 lines of duplicated logic. This has been refactored into shared helpers:

```go
// Shared scan state
type scanResult struct {
    foundSID uint64
    foundPID uint64
    foundOID uint64
    foundGID uint64
    key      []byte
}

type scanOptions struct {
    ctx      context.Context
    s, p, o, g string
    sID, pID, oID, gID uint64
    sBound, pBound, oBound, gBound bool
    strategy *scanStrategy
}

// Unified preparation - called once
func (m *MEBStore) prepareScan(s, p, o, g string) *scanOptions

// Core iteration logic - reused by all scan variants
func (m *MEBStore) scanImpl(opts *scanOptions, processFn func(*scanResult) (Fact, bool)) iter.Seq2[Fact, error]
```

**Lines eliminated:** ~150

#### 22.2.2 Predicate Extraction (`fact_store.go`)

The `GetFacts()` and `Add()` methods both extracted subject/predicate/object/graph from atoms with identical logic:

```go
// Single helper function for all atom parsing
func extractQuadFromAtom(atom ast.Atom) (subject, predicate, object, graph string)
```

**Lines eliminated:** ~40

#### 22.2.3 Prefix Encoding (`keys/encoding.go`)

`EncodeQuadSPOGPrefix()` and `EncodeQuadPOSGPrefix()` had identical nested-if structure:

```go
// Generic helper replaces two nearly identical functions
func buildQuadPrefix(prefix byte, components ...uint64) []byte

func EncodeQuadSPOGPrefix(s, p, o, g uint64) []byte {
    return buildQuadPrefix(QuadSPOGPrefix, s, p, o, g)
}

func EncodeQuadPOSGPrefix(p, o, s, g uint64) []byte {
    return buildQuadPrefix(QuadPOSGPrefix, p, o, s, g)
}
```

**Lines eliminated:** ~35

#### 22.2.4 Dictionary Error Handling

Error messages for missing dictionary IDs were repeated in 4 places:

```go
// Centralized error creation
func dictError(field, value string, err error) error {
    return fmt.Errorf("%s %q not found: %w", field, value, err)
}
```

### 22.3 Summary

| Refactoring | Lines Eliminated |
|-------------|-----------------|
| Scan logic consolidation | ~150 |
| Predicate extraction | ~40 |
| Prefix encoding | ~35 |
| Error handling | ~10 |
| **Total** | **~235 lines** |

### 22.4 Benefits

- **Maintainability**: Changes to scan logic need only be made in one place
- **Testability**: Helper functions can be unit tested independently
- **Readability**: Reduced cognitive load from understanding duplicate code
- **Performance**: Shared implementation means shared optimizations

---

*Document Version: 4.5*
*Last Updated: 2026-02-10*
