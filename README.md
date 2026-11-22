# meb

[![CI](https://github.com/duynguyendang/meb/actions/workflows/ci.yml/badge.svg)](https://github.com/duynguyendang/meb/actions/workflows/ci.yml)

Mangle Extension for Badger — an embedded knowledge graph database combining triple-store semantics with Hybrid (FWHT + Block-wise) vector search.

## Features

- **Triple Store**: Subject-Predicate-Object with dual SPO/OPS indexing (25-byte keys)
- **Multi-Topic Isolation**: 24-bit TopicID bit-packing enables 16M namespaces without a Graph column, enforced across `Scan`, `ScanInTopic`, `Query`, and `SearchInTopic`
- **ANN Indexes**: IVF-PQ (coarse K-means + residual PQ with per-centroid residual ADC) and HNSW (hierarchical navigable small world) for O(log N) vector search
- **Badger-Native Vector Store**: Compressed vectors stored in BadgerDB with mmap cache layer — disk-scaled, O(k) memory per search
- **SIMD ADC Accumulation**: Asymmetric distance computation with scalar, SSE (amd64, opt-in via `MEB_AVX2_ADC=1`), and NEON (arm64) kernels; runtime dispatch via `golang.org/x/sys/cpu`
- **Dual-Path Search**: Hot queries hit parallel mmap (~500K vectors/sec); cold start streams from Badger
- **LSM-Level Topic Filtering**: `SearchInTopic()` and `EncodeSPOTopicPrefix()` use Badger prefix scans — zero I/O on unrelated topics
- **Hybrid Vector Compression**: FWHT preconditioning + block-wise 4/8-bit quantization preserving full 1536 dimensions
- **Cauchy-Schwarz Pruning**: Per-block L2 norm suffix sums for early termination during vector search
- **Zero-Copy Streaming**: Go 1.23+ `iter.Seq2` for constant-memory scan operations
- **Unified BadgerDB**: Single database for graph, dictionary, vectors, and content — enables cross-subsystem transactions
- **Cross-Subsystem Transactions**: Opt-in `View()`/`Update()` API for atomic multi-operation writes (graph + vector + content + dictionary)
- **High-Level Datalog Engine**: `MEBStore.Query()` with single-atom scans, multi-way LFTJ joins, constraint evaluation (`!=`, `=`, `gt`, `gte`, `lt`, `lte`, `contains`, `regex`, `not(triples(...))`), topic/limit-isolated `QueryCache`, and circuit breaker protection
- **Mangle Integration (`mangle-go v0.7.0`)**: `MangleFactStore` (`factstore.FactStoreWithRemove` + typed constants + `SimpleInMemoryStore` overlay) plus `EvalMangle`, `ExplainMangle`, and `ExplainMangleAsFacts` for stratified evaluation, builtins (`:string:matches`, `:float:gt`, `fn:max` on lists), and full `provenance` proof DAGs; lower-level `adapter/` for direct Badger iteration
- **Graph Engine (`meb/graph`)**: Directed graph over `uint64` dictionary IDs with Dijkstra shortest path, BFS reachability, lowest common ancestor, cycle detection, degree centrality, and PageRank
- **Neuro-Symbolic Search**: Hybrid vector + LFTJ graph query builder with streaming joins
- **Rule-Based Optimizer (RBO)**: Strategy selector (`GraphFirst`, `VectorFirst`, `IntersectionFirst`, `PureVectorSearch`, `PureLFTJ`)
- **Cost-Based Optimizer (CBO)**: Greedy smallest-first join ordering with thread-safe `O(1)` LRU/TTL plan cache
- **Streaming Execution**: Concurrent dense/graph path merge with early termination via context cancellation
- **Circuit Breaker**: Configurable query timeout protection (`Query()` + builder paths) with push telemetry
- **Hybrid WAL Approach**: Transactions use BadgerDB `SyncWrites: true` for durability; WriteBatch path uses WAL v2 (CRC32C) for crash recovery
- **Deterministic LFTJ Joins**: Canonical ordering + `ExecuteOrdered` for reproducible multi-way join results
- **Sharded Dictionary**: Configurable lock-striping for high-concurrent ingestion
- **Soft Delete & Compaction**: HNSW tombstone keys + mutex-serialized `Compact()` pruning deleted neighbor links and re-selecting entry points
- **S2 Compression**: Fast content storage with Snappy-compatible compression
- **Ergonomic Presets & Health**: `OpenDefault`, `OpenSafeServing`, `OpenInMemory`, `BatchWriter`, and `Health()` diagnostics

## Quick Start

```go
package main

import (
    "context"
    "log"
    "github.com/duynguyendang/meb"
    "github.com/duynguyendang/meb/store"
)

func main() {
    ctx := context.Background()
    // Or use presets: meb.OpenDefault("./meb-data"), meb.OpenSafeServing("./meb-data"), meb.OpenInMemory()
    cfg := store.DefaultConfig("./meb-data")
    s, err := meb.NewMEBStore(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer s.Close()

    // Add facts
    s.AddFact(meb.NewFact("Alice", "knows", "Bob"))
    s.AddFact(meb.NewFact("Bob", "works_at", "Acme"))

    // High-level Datalog query (LFTJ join + constraints + circuit breaker)
    rows, err := s.Query(ctx, `triples(?a, "knows", ?b), triples(?b, "works_at", ?c), ?c != "Other"`)
    if err != nil {
        log.Fatal(err)
    }
    _ = rows

    // Add document with explicit topic isolation (atomic: dict + facts + vector + content)
    topicID := uint32(101)
    s.AddDocumentWithTopic(topicID, "auth:Login", sourceCode, embedding, metadata)

    // Cross-subsystem transaction (opt-in, all-or-nothing)
    s.Update(func(txn *meb.StoreTxn) error {
        id, _ := txn.GetOrCreateID("entity:Foo")
        txn.AddFact(meb.Fact{Subject: "entity:Foo", Predicate: "type", Object: "class"})
        txn.AddVector(id, embedding)
        txn.SetContent(id, sourceCode)
        return nil // any error rolls back everything
    })

    // Hybrid search limited to a specific topic
    results, _ := s.Find().
        InTopic(topicID).
        SimilarTo(embedding).
        Limit(5).
        Execute(ctx)

    // Scan (zero-copy streaming, constant memory)
    for f, err := range s.Scan("Alice", "", "") {
        if err != nil {
            log.Fatal(err)
        }
        log.Println(f.String())
    }
}
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        MEB Store                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Triple Store          ContentStore      VectorStore       │ │
│  │  (SPO/OPS dual idx)   (S2 blobs)        (Badger-native +  │ │
│  │                                          mmap cache layer) │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Query Builder         │  LFTJ Engine                      │ │
│  │  (Neuro-Symbolic)      │  (Streaming joins, visit limits)  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Telemetry (Push)    │  WAL (Crash Recovery)               │ │
│  │  Circuit/GC/Events   │  Single-DB atomicity                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Transaction API (Opt-in)                                   │ │
│  │  View() / Update() — atomic across all subsystems           │ │
│  │  Auto-rollback on error, counter recovery                  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Unified BadgerDB (Graph + Dict + Vectors + Content)       │ │
│  │  Key prefixes: 0x10 content | 0x11 vectors | 0x20/0x21     │ │
│  │  graph | 0x80/0x81 dict | 0xFF system                     │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Hybrid Retrieval Strategy

MEB's Neuro-Symbolic pipeline combines neural vector search with symbolic graph traversal:

```
1. Semantic Seed (Neuro)
   ┌─────────────────────────────────────────────────────────┐
   │ Dual-path vector search:                                │
   │  • Hot path: Parallel mmap scan in RAM (~500K vectors/s)│
   │  • Cold path: Badger iterator streaming (~50-100K v/s)  │
   │  • Memory: O(k) for top-k heap, never O(N)              │
   │  • Topic search: LSM-level prefix scan skips            │
   │    unrelated topics at storage level                    │
   └────────────────────────┬────────────────────────────────┘
                            │ candidate IDs
                            ▼
2. Structural Expansion (Symbolic)
   ┌─────────────────────────────────────────────────────────┐
   │ LFTJ engine uses seeds to traverse SPO/OPS              │
   │ indices for relational context — streaming, no material │
   └────────────────────────┬────────────────────────────────┘
                            │ filtered triples
                            ▼
3. Lazy Hydration
   ┌─────────────────────────────────────────────────────────┐
   │ Only final filtered results are decompressed from        │
   │ the S2 ContentStore. No unnecessary I/O.                │
   └─────────────────────────────────────────────────────────┘
```

### Key Encoding & ID Structure

MEB uses a **Symmetric TopicID packing** strategy to enable multi-tenancy within a standard triple-store model — no Graph column needed.

```
Prefix    │ Index       │ Key Size │ Value Size  │ ID / Key Layout
──────────┼─────────────┼──────────┼─────────────┼─────────────────────────────────────
0x20      │ SPO         │ 25 bytes │ 16 bytes    │ [Topic:24] | [Local:40]
0x21      │ OPS         │ 25 bytes │ 16 bytes    │ [Topic:24] | [Local:40]
0x10      │ Chunk       │ 9 bytes  │ S2 blob     │ [Local:40]
0x11      │ Vec         │ 9 bytes  │ compressed  │ [Topic:24] | [Local:40]
0x12      │ HNSW / Tomb │ 10 bytes │ neighbors   │ [packedID:8][level:1] / [0xFF][id:8]
0x13      │ IVFCentroid │ 9 bytes  │ float32[]   │ [TopicID:4][CentroidID:4]
0x14      │ IVFPosting  │ 14 bytes │ PQ codes    │ [TopicID:4][CentroidID:4][LocalID:5]
0x15      │ IVFCodebook │ 5 bytes  │ float32[]   │ [TopicID:4]
0x16      │ IVFRawVec   │ 9 bytes  │ float32[]   │ [Topic:24] | [Local:40]
0x80      │ Dict→       │ var      │ 8 bytes     │ string → ID
0x81      │ Dict←       │ 9 bytes  │ var         │ ID → string
0xFF 0x01 │ FactCount   │ 2 bytes  │ 8 bytes     │ durable fact count
0xFF 0x02 │ SchemaVer   │ 2 bytes  │ 8 bytes     │ schema version (2)
```

**ID Bit-packing:**

```
ID = (TopicID << 40) | LocalID

  TopicID (24-bit): Supports up to 16M isolated namespaces/repositories.
  LocalID (40-bit): Supports over 1 trillion entities per topic.

  ┌──────────────────────┬────────────────────────────────────────────┐
  │   TopicID (24 bits)  │             LocalID (40 bits)              │
  │   bits 63–40         │             bits 39–0                     │
  └──────────────────────┴────────────────────────────────────────────┘
```

**Value Format:** `[Vector_ID(8) | Content_Offset(8)]`

The `Content_Offset` uses the lower 48 bits, leaving 16 bits for semantic hints (type, hash).

### Zero-Copy Streaming

All scan operations leverage Go 1.23+ `iter.Seq2` for zero-copy, constant-memory traversal:

```go
// O(1) memory regardless of result set size
// No intermediate slice allocation — results stream directly
for fact, err := range store.Scan("subject", "", "") {
    // Process each fact as it arrives
}
```

- **Constant Memory**: $O(1)$ usage regardless of millions of triples
- **Cloud Run Safe**: No OOM on constrained instances
- **Lazy Evaluation**: Facts materialized only when consumed

### Hybrid Vector Compression

FWHT preconditioning spreads energy uniformly, making block-wise quantization stable across all 1536 dimensions:

```
Input: 1536-d float32 (OpenAI)
  ↓
Pad to next power of 2 (2048)
  ↓
FWHT (Fast Walsh-Hadamard Transform) — O(N log N), in-place
  ↓
Normalize by 1/√N (unitary)
  ↓
Block-wise Quantize: 64 blocks of 32 elements
  ↓
Per block: [scale:4B][zero:4B][norm:4B][q_0:1B]...[q_31:1B]
  ↓
Compressed: 2,816 bytes (8-bit) or 1,792 bytes (4-bit)
```

The per-block L2 norm enables **Cauchy-Schwarz early termination**: suffix sums of `normA[r] × normB[r]` provide an upper bound on remaining dot product contribution, allowing pruning of dissimilar vectors after scanning only 10-20 of 64 blocks.

| BitWidth | Compressed Size | Compression | RAM for 1M vectors |
|----------|----------------|-------------|---------------------|
| 8-bit | 2,816 bytes | 2.2x | 2.82 GB |
| 4-bit | 1,792 bytes | 3.4x | 1.79 GB |
| float32 | 6,144 bytes | 1x | 6.14 GB |

### Vector Search Architecture

MEB uses a **dual-path** vector retrieval strategy for optimal performance at any scale:

```
Search() / SearchWithFilter()
    │
    ├─ mmap has vectors? (totalVectors > 0)
    │   └─ YES → 4-way parallel mmap scan (~500K vectors/sec)
    │            Cache-hot sequential RAM access
    │
    └─ NO → Badger iterator streaming (~50-100K vectors/sec)
            Warms mmap cache as it goes — next search hits fast path

SearchInTopic(topicID)
    └─ Badger prefix scan on [0x11][topicID]
       LSM-level topic filtering — zero I/O on unrelated topics
       Memory: O(k) for top-k heap, never O(N)
```

**Benefits:**
- **Hot queries**: Parallel mmap scan at ~500K vectors/sec
- **Cold start**: Streams from Badger, no RAM limit
- **Topic filtering**: Skips 99% of data at storage level
- **Write durability**: Synchronous Badger write before return
- **Scale**: Limited by disk size, not RAM

**Blockwise Dot Product** — similarity computed directly on compressed data without full dequantization:

```
dot(a, b) = Σ_blocks (scale_a * scale_b * Σ(q_a_i * q_b_i)
                      + scale_a * zero_b * Σ(q_a_i)
                      + scale_b * zero_a * Σ(q_b_i)
                      + block_size * zero_a * zero_b)
```

**Pruning** — per-block norm suffix sums enable Cauchy-Schwarz early termination:

```
suffixNormProduct[b] = Σ_{r>=b} normA[r] * queryNorms[r]

for each block b:
    totalSum += blockContribution
    if totalSum + suffixNormProduct[b+1] < threshold:
        return totalSum  // early exit — impossible to beat kth-best
```

## Cross-Subsystem Transactions

MEB provides **opt-in transactions** for atomic operations across graph, dictionary, vectors, and content. The existing API (`AddFact`, `AddDocument`, etc.) works unchanged; transactions are an additional layer for complex use cases.

```go
// Atomic multi-operation: all succeed or all rollback
err := store.Update(func(txn *meb.StoreTxn) error {
    // Dictionary
    id, err := txn.GetOrCreateID("entity:UserService")

    // Graph facts
    txn.AddFact(meb.Fact{Subject: "entity:UserService", Predicate: "type", Object: "class"})
    txn.AddFact(meb.Fact{Subject: "entity:UserService", Predicate: "package", Object: "com.example"})

    // Vector embedding
    txn.AddVector(id, embedding)

    // Content
    txn.SetContent(id, sourceCode)

    return nil // any error → automatic rollback
})

// Read-only transaction
store.View(func(txn *meb.StoreTxn) error {
    for f, err := range txn.Scan(ctx, "entity:UserService", "", "") {
        // ...
    }
    return nil
})
```

**Transaction guarantees:**
- **Atomicity**: All writes commit together, or none do
- **Rollback**: Any error discards all writes in the transaction
- **Counter recovery**: `numFacts` counter is restored on rollback
- **Isolation**: Uses BadgerDB's MVCC for read/write isolation

**When to use transactions:**
| Scenario | Use `AddFact()`/`AddDocument()` | Use `Update()` |
|----------|--------------------------------|----------------|
| Single fact | ✅ | |
| Single document | ✅ | |
| Multiple subsystems (dict + facts + vector + content) | | ✅ |
| Conditional writes | | ✅ |
| Rollback on error | | ✅ |

### Hybrid WAL Approach

MEB uses a **hybrid WAL strategy** to balance durability with performance:

| Write Path | Durability Mechanism | Reason |
|------------|---------------------|--------|
| **Transactions** (`Update()`/`View()`) | BadgerDB `SyncWrites: true` | BadgerDB handles transaction durability natively; WAL is redundant |
| **WriteBatch** (`AddFactBatch()`) | WAL v2 (CRC32C) | BadgerDB `WriteBatch` lacks `SyncWrites`; WAL provides crash recovery |

**Configuration:**
- `DefaultConfig()` → `SyncWrites: true` (ingest-heavy, durability-focused)
- `SafeServingConfig()` → `SyncWrites: false` (read-only/serve)
- `ReadOnlyConfig()` → `SyncWrites: false` (read-only)

**WAL features:**
- CRC32C checksums for corruption detection
- `replayWAL()` for WriteBatch crash recovery (not used for transactions)
- `TruncateIncompleteWAL()` for cleaning up partial writes

**When WAL is used:**
- WriteBatch path (e.g., bulk ingestion with `AddFactBatch`)
- Transactions use BadgerDB's native durability (no WAL)

### Background Orphan Cleanup

MEB runs periodic cleanup of orphaned data during `runCleanup()`:

1. **Deprecated triples** — Scans SPO index, deletes entries with `FlagIsDeprecated`
2. **Orphan dictionary entries** — Scans reverse dictionary, removes entries not referenced by any fact
3. **Orphan vectors** — Two-pass scan: collects all subject IDs from facts, then removes vectors with no content and no referencing facts
4. **ValueLog GC** — Runs BadgerDB GC to reclaim space from tombstones

All cleanup operations are **throttled** (max 1000 orphans per cycle) to avoid long GC pauses.

## Package Structure

```
meb/
├── Makefile               # SIFT-1M download & benchmark targets
├── keys/                  # 25-byte triple key encoding, TopicID/inline bit-packing, IVF/HNSW keys
├── dict/                  # String interning (thread-safe LRU + sharded range allocator)
├── store/                 # BadgerDB config with deployment profiles
├── graph/                 # Integer-ID graph algorithms (Dijkstra, BFS, LCA, cycles, centrality, PageRank)
├── vector/                # Hybrid vector compression, ANN indexes & SIMD ADC
│   ├── turboquant.go      # FWHT + block-wise 4/8-bit quantization + Cauchy-Schwarz pruning
│   ├── fwht.go            # Fast Walsh-Hadamard transform
│   ├── partitioned.go     # PartitionedRegistry (sharded by TopicID)
│   ├── registry.go        # Badger-native store + mmap cache, RCU revMap
│   ├── mmap*.go           # Cross-platform mmap segment lifecycle
│   ├── search.go          # Dual-path: mmap parallel + Badger streaming, adaptive workers
│   ├── heap.go            # Purpose-built top-K and node-distance heaps
│   ├── math.go            # L2 normalize, dot product
│   ├── index.go           # Index selection API (brute-force / IVF-PQ / HNSW)
│   ├── ivfpq.go           # IVF-PQ index: centroid assignment, residual ADC search, posting scan
│   ├── ivfpq_config.go    # IVFPQConfig with validation
│   ├── ivfpq_train.go     # Mini-batch K-means training, PQ codebook training
│   ├── hnsw.go            # HNSW index: Insert, SearchInTopic, searchLayer
│   ├── hnsw_graph.go      # HNSW graph operations: writeNode, readNeighbors, deleteNode
│   ├── hnsw_level.go      # Level lookup helpers
│   ├── hnsw_delete.go     # SoftDelete, isDeleted, Compact
│   ├── adc_*.go / *.s     # Scalar, SSE (amd64), and NEON (arm64) ADC kernels + dispatch
│   ├── dotprod_*.go       # Platform dot-product helpers
│   └── diskann.go         # DiskANNConfig and DiskANNIndex scaffold (unreachable)
├── query/                 # LFTJ engine + Datalog parser + CBO + RBO + plan/query caches
│   ├── lftj.go            # TrieIterator, LFTJResult, Canonical ordering
│   ├── datalog_parser.go  # Datalog atom parser & constraint syntactic sugar
│   ├── cache.go           # TTL query result cache
│   ├── engine.go          # Execute, ExecuteOrdered, WithBufferAndSort, OptimizeRelations
│   ├── rbo.go             # Rule-Based Optimizer (PlanType, ExecutionPlan, Optimize)
│   ├── plan_cache.go      # QueryPlanCache with container/list LRU + TTL + FNV-1a hash
│   └── stream.go          # ExecuteStream concurrent dense/sparse merge
├── circuit/               # Query timeout circuit breaker with state callbacks & metrics
├── utils/                 # Zero-copy string/byte conversion
├── adapter/               # Mangle Datalog integration
├── bench/                 # ANN & CBO benchmarks + SIFT dataset loader
├── cmd/bench/             # Benchmark runner CLI
├── store.go               # MEBStore orchestrator, factory presets, EnableIVFPQ/EnableHNSW
├── tx.go                  # Transaction API (View/Update, StoreTxn, AddIVFVector, SearchHybrid)
├── knowledge_store.go     # SPO/OPS dual-index write, orphan cleanup
├── scan.go                # Index & topic-scoped scan (iter.Seq2 streaming)
├── datalog.go             # High-level MEBStore.Query engine with constraints & breaker
├── content.go             # S2-compressed content storage, atomic Add/DeleteDocument
├── batch.go               # Bulk point reads and BatchWriter
├── health.go              # StoreHealth diagnostics and manual compaction
├── query_builder.go       # Neuro-symbolic query builder with LFTJ joins — Execute(ctx)
├── query_result.go        # Result and Results helpers
├── fact.go / errors.go    # Fact value type and sentinel errors
├── generics.go / helpers.go # iter.Seq2 helpers and batching primitives
├── telemetry.go           # Push telemetry (TelemetrySink interface)
├── wal.go                 # Write-ahead log v2 (CRC32C, WriteBatch crash recovery)
├── fact_store.go          # factstore.FactStore implementation
└── go.mod                 # Go module dependencies (go.uber.org/goleak, rapid, etc.)
```

## Configuration

```go
// Default (Ingest-Heavy)
cfg := store.DefaultConfig("./data")
// SyncWrites: true (transactions use BadgerDB durability)

// Cloud Run optimized
cfg := store.SafeServingConfig("./data")
// SyncWrites: false (read-only/serve mode)

// Read-only
cfg := store.ReadOnlyConfig("./data")
// SyncWrites: false (read-only mode)

// Enable verbose debug logging
cfg.Verbose = true

// Enable sharded dictionary (must be power of 2)
cfg.NumDictShards = 4

// Set vector default dimensionality (default: 1536)
cfg.VectorFullDim = 1536
```

## Observability

MEB provides **push telemetry** for autonomous agent consumption:

```go
// Register a telemetry sink
store.RegisterTelemetrySink(&mySink{})

// Events emitted automatically:
//   "circuit_state_change" — circuit breaker transitions
//   "gc_failure" — ValueLogGC errors
//   "retention" — fact count exceeds threshold
//   "deprecated_cleanup" — deprecated triples purged
//   "dict_orphan_cleanup" — orphaned dictionary entries removed
//   "vector_orphan_cleanup" — orphaned vectors removed
//   "wal_clear_failed" — WAL consistency issue
```

## Performance

**Benchmark results** (AMD Ryzen 9 5900HS with Radeon Graphics, in-memory BadgerDB, Go 1.23):

| Benchmark | Ops/sec | Latency | Memory | Allocs |
|-----------|---------|---------|--------|--------|
| **Fact Insertion** (single) | 33,507 | 35.0 µs/op | 8.3 KB/op | 192 |
| **Fact Insertion** (batch × 10) | 9,794 | 134.6 µs/op | 45.2 KB/op | 1,042 |
| **Fact Insertion** (batch × 100) | 1,291 | 1,094 µs/op | 424 KB/op | 9,503 |
| **Fact Insertion** (batch × 1000) | 100 | 10.6 ms/op | 4.7 MB/op | 93,330 |
| **Document Add** (content + vector + metadata) | 14,925 | 76.1 µs/op | 33.9 KB/op | 113 |
| **Transaction Batch** (100 facts) | 2,232 | 582 µs/op | 257 KB/op | 5,973 |
| **Scan** (1000 facts, single key) | 21,270 | 54.9 µs/op | 27.2 KB/op | 621 |
| **Vector Add** (1536-d, 8-bit Hybrid) | 24,759 | 47.3 µs/op | 29.9 KB/op | 37 |
| **Vector Search** (10K vectors, k=10, with pruning) | 273 | 4.5 ms/op | 17.5 KB/op | 52 |
| **Dictionary Lookup** (GetOrCreate) | 1,532,017 | 839 ns/op | 400 B/op | 7 |

**Derived throughput:**

| Metric | Value | Notes |
|--------|-------|-------|
| **Vector Ingestion** | ~25K vectors/sec | Includes FWHT + quantization + norm computation + Badger write |
| **Vector Search** | ~2.7M vectors/sec | Pruned mmap scan over 10K vectors, 15% faster than pre-pruning |
| **Fact Ingestion** | ~34K facts/sec (single), ~129K facts/sec (batch × 100) | Dual-index SPO+OPS write |
| **Scan Throughput** | ~21M keys/sec | Key-only, SPO prefix scan |
| **Dictionary Lookup** | ~1.5M lookups/sec | Sharded LRU cache hit |

**Key observations:**
- **Cauchy-Schwarz pruning** speeds up search by 15% at 10K scale (238→273 ops/s), with increasing gains at larger vector counts
- Recall@10 improved from 90.0% to **97.0%** with per-block norm storage
- Vector Add is 1.9x faster than original after eliminating double quantization (previously 15K→28K vectors/sec)
- Vector search latency scales with dimension — 1536-dim is ~40x slower than 128-dim (4.5 ms vs 124 µs)
- Scan latency scales with matching facts, not total graph size (prefix scan)
- Dictionary lookups are sub-microsecond with thread-safe LRU cache
- DeleteFactsBySubject uses scoped prefix scans (not full graph scan) for orphan cleanup
- Storage overhead: +10% per vector (2,560→2,816 bytes) for per-block norm pruning

**Storage scaling** (per 1M items):

| Mode | Storage | Components Used |
|------|---------|----------------|
| **Facts only** | ~150 MB | SPO(41) + OPS(41) + Dict(23) per fact |
| **+ 100K vectors** (8-bit) | ~430 MB | Facts(150) + Vectors(282) |
| **+ 100K vectors + 10K docs** (S2) | ~530 MB | Facts(150) + Vectors(282) + Content(100) |

- Facts-only mode uses minimal storage — no overhead from unused subsystems
- Vectors dominate storage (4-5x larger than facts at 8-bit)
- Content is moderate if documents are few (S2 compressed)

| Metric | Value | Notes |
|--------|-------|-------|
| **RAM Density** | Up to 1.1M nodes (1536-d) | Within 2GB RAM using Hybrid 4-bit |
| **Disk-Scaled** | Unlimited vectors | Badger-backed storage — not RAM-limited |
| **Cold Start** | < 200ms warm-up | Safe-Serving profile |
| **Join Latency** | Sub-2s | Complex code-graph traversals with circuit breaker |
| **Vector Search (hot, pruned)** | ~273K vectors/sec | mmap parallel scan with Cauchy-Schwarz pruning |
| **Vector Search (cold)** | ~76M vectors/sec | Badger iterator streaming |
| **Topic Search** | LSM prefix scan only | Zero I/O on unrelated topics |
| **Fact Insertion** | ~102K facts/sec (single), ~861K facts/sec (batch) | Dual-index write |
| **Scan Throughput** | ~102M keys/sec | Key-only, SPO prefix scan |
| **Content Read** | ~500MB/s | S2 decompression |

## Fidelity Verification

Five stress-tested metrics validate the Hybrid (FWHT + Block-wise) quantization under **Gaussian distribution with extreme outliers** (50x-100x spikes):

| Metric | Test | Result | Target | Status |
|--------|------|--------|--------|--------|
| **Mathematical Fidelity** | `TestHybridQuantizationFidelity` | 8-bit: **1.0000**, 4-bit: **0.9977** | 8-bit > 0.95, 4-bit > 0.90 | ✅ |
| **Dot Product Accuracy** | `TestHybridQuantizationFidelity` | 8-bit MAE: **0.18**, 4-bit MAE: **2.97** | Rank-order stability | ✅ |
| **Recall@10** | `TestRecallAtK` (10K vectors, 10 seeds) | **97.0%** avg | > 80% | ✅ |
| **FWHT Invariance** | `TestFWHTInvariance` | Max error < 1e-5 (dims 4-2048) | FWHT(FWHT(v))/N == v | ✅ |
| **Energy Spreading** | `TestQuantizationDistribution` | CV: 0.29 → **0.16** (46% reduction) | avgCVFWHT < avgCVNoFWHT | ✅ |
| **8-bit Lossless** | `Test8BitLosslessVerification` | Max error: **0.063**, BER: **29.8%** | Max error < 1.0, BER < 50% | ✅ |

**Key observations:**
- **8-bit is near-lossless** (cosine = 1.0000) — 256 levels are more than enough for 32-element blocks
- **4-bit fidelity is excellent** (cosine = 0.9977) — far exceeds the 0.90 target even with spikes
- **Recall@10 = 97.0%** — improved from 90.0% with per-block norm storage in quantized vectors
- **FWHT reduces block scale variance by 46%** (CV: 0.29 → 0.16) — proves energy spreading works on high-entropy data
- **Dot product MAE stays low** — 8-bit MAE of 0.18 vs 4-bit MAE of 2.97 on 1536-dim vectors
- **FWHT is mathematically correct** — passes invariance test across all power-of-2 dimensions

Run fidelity tests:
```bash
go test ./vector/... -v -run "TestHybridQuantizationFidelity|TestRecallAtK|TestFWHTInvariance|TestQuantizationDistribution|Test8BitLosslessVerification"
```

**Cloud Run Guardrails:**

| Constraint | Value | Purpose |
|------------|-------|---------|
| Query Circuit Breaker | 2,000ms | Stop runaway queries |
| Max Join Results | 5,000 facts | Prevent RAM exhaustion |
| Vector Search Top-K | 100 | Limit result set size |

## Build & Test

```bash
go build ./...
go test ./...
go test -race . ./adapter/... ./circuit/... ./dict/... ./graph/... ./keys/... ./query/... ./store/... ./vector/...
go vet ./...
make bench            # run benchmarks in ./bench/...
```

## Dependencies

- `github.com/dgraph-io/badger/v4` — Key-value storage
- `codeberg.org/TauCeti/mangle-go` — Datalog reasoning engine
- `github.com/klauspost/compress` — S2 compression
- `github.com/hashicorp/golang-lru/v2` — Dictionary caching
- `go.uber.org/goleak` — Goroutine leak detection in tests
- `golang.org/x/sys` — CPU feature detection (SIMD ADC dispatch)
- `pgregory.net/rapid` — Property-based testing (`keys/`)
