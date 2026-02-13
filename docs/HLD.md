# MEB High Level Design (HLD)

> **Version**: 1.0  
> **Last Updated**: 2026-02-13  
> **Status**: Active  

---

## 1. Executive Summary

MEB (Mangle Extension for Badger) is a high-performance embedded knowledge graph database optimized for code analysis and semantic search. It combines quad-store semantics with vector embeddings to enable hybrid symbolic-neural retrieval in a single, lightweight Go library.

### Key Characteristics

| Characteristic | Implementation |
|---------------|-----------------|
| **Embedded** | No external dependencies, runs in-process |
| **Quad Store** | Subject-Predicate-Object-Graph model |
| **Multi-Tenant** | Graph context for tenant isolation |
| **Concurrent** | Lock-free counters, sharded dictionary |
| **Scalable** | Memory-mapped vector storage |
| **High-Performance** | SIMD vector search, LFTJ joins |

---

## 2. Design Principles

### 2.1 Core Principles

```
┌─────────────────────────────────────────────────────────────────┐
│                      MEB Design Principles                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. STATIC OVER DYNAMIC                                         │
│     └── Compute at ingest time, not query time                   │
│                                                                  │
│  2. LOCAL OVER REMOTE                                           │
│     └── Download to ephemeral disk, no network dependencies     │
│                                                                  │
│  3. DEDUCTIVE OVER IMPERATIVE                                   │
│     └── Rules infer facts, don't execute business logic          │
│                                                                  │
│  4. BOUNDED OVER UNBOUNDED                                      │
│     └── All operations have hard limits                          │
│                                                                  │
│  5. EFFICIENT OVER CONVENIENT                                   │
│     └── Memory efficiency over developer convenience             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Implementation Guardrails

| Guardrail | Value | Purpose |
|-----------|-------|---------|
| Max Join Results | 5,000 facts | Prevent RAM exhaustion |
| Query Circuit Breaker | 2,000ms | Stop runaway queries |
| Vector Search Top-K | 100 | Limit result set size |
| MRL Dimensions | 64 | Fixed search buffer size |
| Leiden Iterations | 10 per level | Prevent non-convergence |

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              MEB Store                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Query Layer                                 │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐  │   │
│  │  │ Query       │  │ Neuro-      │  │ Datalog     │  │ LFTJ    │  │   │
│  │  │ Builder     │  │ Symbolic    │  │ Engine      │  │ Engine  │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Storage Layer                               │   │
│  │                                                                  │   │
│  │  ┌─────────────────────────────────────────────────────────────┐ │   │
│  │  │                    Quad Store                               │ │   │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐               │ │   │
│  │  │  │   SPOG   │  │   POSG   │  │   GSPO   │               │ │   │
│  │  │  │ (forward)│  │ (reverse)│  │ (lifecycle)              │ │   │
│  │  │  │  0x20    │  │  0x21    │  │  0x22    │               │ │   │
│  │  │  └──────────┘  └──────────┘  └──────────┘               │ │   │
│  │  └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                  │   │
│  │  ┌─────────────────────┐    ┌─────────────────────────────────┐ │   │
│  │  │   Vector Registry  │    │       Content Store            │ │   │
│  │  │  INT8 (64-d) mmap  │    │   S2 Compression               │ │   │
│  │  │  Float32 (1536-d)  │    │                                 │ │   │
│  │  └─────────────────────┘    └─────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Dictionary Layer                              │   │
│  │  ┌─────────────────────────────────────────────────────────┐    │   │
│  │  │           Sharded Dictionary (32 shards)                 │    │   │
│  │  │      String ⇄ ID mapping with LRU cache                │    │   │
│  │  └─────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    BadgerDB (Dual Instance)                     │   │
│  │  ┌───────────────────────┐    ┌───────────────────────────┐     │   │
│  │  │  graphDB (async)    │    │  dictDB (sync)           │     │   │
│  │  │  Quads, Vectors    │    │  String↔ID mapping      │     │   │
│  │  │  Content            │    │                         │     │   │
│  │  └───────────────────────┘    └───────────────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Quad Store Key Format

```
33-byte Composite Key (SPOG):
┌────┬────────┬────────┬────────┬────────┐
│ 1B │  8B   │  8B   │  8B   │  8B   │
├────┼────────┼────────┼────────┼────────┤
│prefix│ subject│predicate│ object │ graph │
│0x20 │   S   │   P    │   O   │   G   │
└────┴────────┴────────┴────────┴────────┘

Triple Indexes:
  SPOG (0x20) → Forward traversal:   Find all relations FROM a subject
  POSG (0x21) → Reverse lookup:       Find all subjects TO an object  
  GSPO (0x22) → Lifecycle:             Find all facts IN a graph
```
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Architecture Decision Records (ADRs)

### ADR-001: Use BadgerDB as Primary Storage Engine

**Status**: ✅ Accepted

**Context**:
MEB needs an embedded key-value store with:
- High write throughput
- Memory-mapped reads
- Pure Go implementation (no CGO)

**Decision**:
Use BadgerDB with dual-instance architecture:
- `graphDB`: Async writes for quads/content
- `dictDB`: Sync writes for dictionary

**Consequences**:
- ✅ No external dependencies
- ✅ Excellent write performance
- ✅ Memory-mapped reads
- ❌ Larger disk footprint than LSM-only
- ❌ Value log GC required

---

### ADR-002: Quad-Based Storage with Triple Indexing

**Status**: ✅ Accepted

**Context**:
Need efficient bidirectional graph traversal without full table scans.

**Decision**:
Store all facts as 33-byte quads with three indexes:
- **SPOG** (0x20): Subject-Predicate-Object-Graph - forward traversal
- **POSG** (0x21): Predicate-Object-Subject-Graph - reverse lookups
- **GSPO** (0x22): Graph-Subject-Predicate-Object - graph lifecycle

**Key Format**:
```
[prefix:1][subject:8][predicate:8][object:8][graph:8] = 33 bytes
```

**Consequences**:
- ✅ O(1) prefix scans in both directions
- ✅ 3x storage overhead (acceptable)
- ✅ Query optimizer selects optimal index

---

### ADR-003: Dictionary Encoding for String Interning

**Status**: ✅ Accepted

**Context**:
Storing strings directly wastes memory and slows comparisons.

**Decision**:
Use dictionary encoding with sharded architecture:
- All strings → uint64 IDs
- 32 shards with per-shard LRU cache
- FNV-1a hash for shard distribution

**Consequences**:
- ✅ 8-byte fixed-size keys
- ✅ Reduced memory footprint
- ✅ Faster comparisons
- ❌ Lookup overhead for string resolution

---

### ADR-004: Dual Vector Storage Strategy

**Status**: ✅ Accepted

**Context**:
Need both fast search AND accurate retrieval.

**Decision**:
Store vectors in two formats:
1. **INT8 Quantized (64-d)**: mmap'd flat file for search
2. **Float32 Full (1536-d)**: BadgerDB for retrieval

**Flow**:
```
Search ──▶ INT8 mmap (fast) ──▶ Top-K IDs
              │
              └──▶ Float32 DB ──▶ Re-rank ──▶ Results
```

**Consequences**:
- ✅ 96x compression ratio
- ✅ Sub-millisecond search
- ✅ Accurate final results
- ❌ Two storage locations

---

### ADR-005: Go 1.23 iter.Seq2 for Streaming

**Status**: ✅ Accepted

**Context**:
Need memory-efficient result streaming for large queries.

**Decision**:
Use Go 1.23's `iter.Seq2[T, error]` for all scan operations:
```go
func (m *MEBStore) Scan(s, p, o, g string) iter.Seq2[Fact, error]
```

**Consequences**:
- ✅ Zero allocation during iteration
- ✅ Lazy evaluation
- ✅ Natural for/range syntax
- ❌ Requires Go 1.23+

---

### ADR-006: Leapfrog Triejoin for Multi-Way Joins

**Status**: ✅ Accepted

**Context**:
Traditional nested-loop joins are O(|R₁| × |R₂| × ...), inefficient for large joins.

**Decision**:
Implement Leapfrog Triejoin (LFTJ) with:
- Trie-based intersection
- Worst-case optimal: O(N × |output|)
- Variable ordering optimization

**Consequences**:
- ✅ 10-1000x faster for complex joins
- ✅ No intermediate materialization
- ✅ Streams results directly

---

### ADR-007: Circuit Breaker for Query Protection

**Status**: ✅ Accepted

**Context**:
Unbounded queries can exhaust RAM or run indefinitely.

**Decision**:
Implement circuit breaker with:
- 2-second hard timeout
- Failure/success thresholds
- Half-open state for recovery

**Consequences**:
- ✅ Prevents runaway queries
- ✅ System stability
- ❌ Some queries may be rejected

---

### ADR-008: Little-Endian Binary Encoding

**Status**: ✅ Accepted

**Context**:
Need architecture-neutral binary format for mmap compatibility.

**Decision**:
Use Little-Endian encoding with 8-byte alignment:
- Native on x86_64 (zero-cost)
- Explicit on ARM64 (portable)
- 8-byte aligned prevents bus errors

**Consequences**:
- ✅ Cross-platform compatibility
- ✅ SIMD-friendly
- ✅ Safe mmap access

---

### ADR-009: Leiden Algorithm for Community Detection

**Status**: ✅ Accepted

**Context**:
Need static clustering at ingest time, not real-time.

**Decision**:
Implement 3-pass Leiden algorithm:
1. Local moving phase
2. Refinement phase
3. Aggregation phase

With hard limits:
- Max 10 iterations per level
- Convergence detection
- CSR graph representation

**Consequences**:
- ✅ Quality community detection
- ✅ Bounded computation time
- ✅ Hierarchical communities

---

### ADR-010: Zero-Copy Page-Aligned Retrieval

**Status**: ✅ Accepted

**Context**:
Traditional reads involve allocation + memcpy from kernel pages.

**Decision**:
Implement zero-copy with:
- Unsafe pointer access to mmap'd pages
- Reference counting for lifecycle
- LRU page cache

**Performance**:
- 10x throughput improvement
- Zero allocation per fact
- Zero memcpy overhead

---

## 5. Module Design

### 5.1 Core Packages

```
meb/
├── store.go           # MEBStore main entry point
├── fact.go            # Fact/Quad data model
├── scan.go            # Quad scanning with index selection
├── query_builder.go   # Neuro-symbolic query builder
├── content.go         # Content storage with S2 compression
├── keys/
│   └── encoding.go    # Quad key encoding (33-byte)
├── dict/
│   ├── sharded.go     # Sharded dictionary encoder
│   └── interface.go   # Dictionary interface
├── vector/
│   ├── registry.go    # Vector registry
│   ├── search.go      # SIMD search
│   ├── quantization.go # INT8 quantization
│   └── simd_int8.go  # SIMD dot product
├── clustering/
│   ├── csr.go        # CSR graph representation
│   └── leiden.go     # Leiden algorithm
├── storage/
│   ├── zerocopy.go   # Zero-copy buffer
│   └── page_cache.go # LRU page cache
├── query/
│   ├── lftj.go       # Leapfrog Triejoin
│   └── engine.go     # Query engine
└── circuit/
    └── breaker.go    # Circuit breaker
```

### 5.2 Package Responsibilities

| Package | Responsibility | Public API |
|---------|----------------|------------|
| `meb` | Store facade | `NewMEBStore`, `Scan`, `Find` |
| `keys` | Key encoding | `EncodeQuadKey`, `DecodeQuadKey` |
| `dict` | String↔ID | `GetOrCreateID`, `GetString` |
| `vector` | Vector ops | `Add`, `Search`, `GetFullVector` |
| `clustering` | Communities | `Detect`, `GetCommunityMembers` |
| `storage` | Zero-copy | `ZeroCopyBuffer`, `PageCache` |
| `query` | Joins | `LFTJEngine`, `ExecuteLFTJQuery` |
| `circuit` | Protection | `Execute`, `Metrics` |

---

## 6. Data Flow

### 6.1 Write Path

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  AddFact     │────▶│  Dictionary  │────▶│  Quad Keys  │
│  (user)      │     │  (String→ID) │     │  (SPO/OPS) │
└──────────────┘     └──────────────┘     └──────────────┘
                                               │
                     ┌─────────────────────────┼─────────────────────────┐
                     │                         │                         │
                     ▼                         ▼                         ▼
              ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
              │   graphDB    │         │   graphDB    │         │   vector     │
              │  (async)    │         │  (async)     │         │  (mmap INT8) │
              └──────────────┘         └──────────────┘         └──────────────┘
```

### 6.2 Read Path

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Scan        │────▶│  Index       │────▶│  Iterator    │
│  (query)     │     │  Selection   │     │  (lazy)      │
└──────────────┘     └──────────────┘     └──────────────┘
                                               │
                     ┌─────────────────────────┼─────────────────────────┐
                     │                         │                         │
                     ▼                         ▼                         ▼
              ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
              │   Facts      │         │   Content    │         │   Vectors    │
              │   (yield)    │         │   (S2 dec)  │         │   (SIMD)     │
              └──────────────┘         └──────────────┘         └──────────────┘
```

---

## 7. Performance Characteristics

### 7.1 Latency Targets

| Operation | P50 | P95 | P99 |
|-----------|-----|-----|-----|
| Vector Search (SQ8) | 0.34ms | 0.59ms | 0.94ms |
| Graph Scan (Dual-Index) | 0.01ms | 0.02ms | 0.03ms |
| Metadata Lookup | 0.01ms | 0.01ms | 0.03ms |
| Mixed Query (RAG) | 0.74ms | 3.92ms | 23.52ms |

### 7.2 Throughput

| Operation | Throughput |
|-----------|------------|
| Vector Search | ~2,900 ops/sec |
| Graph Scan | ~142,800 ops/sec |
| Metadata Lookup | ~200,000 ops/sec |

### 7.3 Compression Ratios

| Data Type | Raw | Compressed | Ratio |
|-----------|-----|------------|-------|
| Source Code | 100MB | 35MB | 2.8:1 |
| Quads (SPOG) | 500MB | 120MB | 4.2:1 |
| Vectors (1536-d) | 6GB | 64MB | 96:1 |
| **Total** | 6.6GB | 219MB | **30:1** |

---

## 8. Configuration Profiles

### 8.1 Development Profile

```go
opts := &store.Config{
    DataDir:        "./data",
    InMemory:       false,
    BlockCacheSize: 256 << 20,  // 256MB
    IndexCacheSize: 128 << 20,  // 128MB
    LRUCacheSize:   10000,
    NumDictShards:  4,
}
```

### 8.2 Production Profile (Read-Heavy)

```go
opts := &store.Config{
    DataDir:        "/mnt/nvme/meb",
    InMemory:       false,
    BlockCacheSize: 8 << 30,    // 8GB
    IndexCacheSize: 1 << 30,    // 1GB
    LRUCacheSize:   100000,
    NumDictShards:  32,
    Compression:     true,
    SyncWrites:     false,
}
```

### 8.3 Cloud Run Profile (Low Memory)

```go
opts := &store.Config{
    DataDir:        "/tmp/meb",
    InMemory:       false,
    BlockCacheSize: 64 << 20,   // 64MB
    IndexCacheSize: 256 << 20,   // 256MB
    LRUCacheSize:   5000,
    NumDictShards:  8,
    ReadOnly:       false,
}
```

---

## 9. Migration Guide

### 9.1 From Triple to Quad

**Before** (v1):
```go
// 25-byte triple key
key := []byte{0x10} + subject + predicate + object
```

**After** (v2):
```go
// 33-byte quad key
key := []byte{0x20} + subject + predicate + object + graph
```

### 9.2 From Iterator to iter.Seq2

**Before**:
```go
facts, err := store.GetFacts(predicate)
for _, fact := range facts {
    // process
}
```

**After**:
```go
for fact, err := range store.Scan("subject", "", "", "") {
    if err != nil {
        return err
    }
    // process
}
```

---

## 10. References

- [CSD.md](./CSD.md) - Detailed implementation specifications
- [TO-DO.md](./TO-DO.md) - Implementation progress
- [BadgerDB Documentation](https://dgraph.io/docs/badger)
- [Leiden Algorithm](https://arxiv.org/abs/1810.08403)
- [Leapfrog Triejoin](https://arxiv.org/abs/1407.2045)
