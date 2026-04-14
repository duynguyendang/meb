# meb

Mangle Extension for Badger — an embedded knowledge graph database combining triple-store semantics with Hybrid (FWHT + Block-wise) vector search.

## Features

- **Triple Store**: Subject-Predicate-Object with dual SPO/OPS indexing (25-byte keys)
- **Multi-Topic Isolation**: 24-bit TopicID bit-packing enables 16M namespaces without a Graph column
- **Badger-Native Vector Store**: Compressed vectors stored in BadgerDB with mmap cache layer — disk-scaled, O(k) memory per search
- **Dual-Path Search**: Hot queries hit parallel mmap (~500K vectors/sec); cold start streams from Badger
- **LSM-Level Topic Filtering**: `SearchInTopic()` uses Badger prefix scan — zero I/O on unrelated topics
- **Hybrid Vector Compression**: FWHT preconditioning + block-wise 4/8-bit quantization preserving full 1536 dimensions
- **Zero-Copy Streaming**: Go 1.23+ `iter.Seq2` for constant-memory scan operations
- **Dual BadgerDB**: Separate graph and dictionary databases for performance isolation
- **Datalog Integration**: Mangle `factstore.FactStore` interface for symbolic reasoning
- **Neuro-Symbolic Search**: Hybrid vector + LFTJ graph query builder with streaming joins
- **Circuit Breaker**: Configurable query timeout protection with push telemetry
- **Write-Ahead Log**: Dual-DB atomicity guarantees via file-based WAL with crash recovery
- **Sharded Dictionary**: Configurable lock-striping for high-concurrent ingestion
- **S2 Compression**: Fast content storage with Snappy-compatible compression

## Quick Start

```go
package main

import (
    "log"
    "github.com/duynguyendang/meb"
    "github.com/duynguyendang/meb/store"
)

func main() {
    cfg := store.DefaultConfig("./meb-data")
    s, err := meb.NewMEBStore(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer s.Close()

    // Add facts
    s.AddFact(meb.NewFact("Alice", "knows", "Bob"))
    s.AddFact(meb.NewFact("Alice", "works_at", "Acme"))

    // Add document with explicit topic isolation
    topicID := uint32(101)
    s.AddDocumentWithTopic(topicID, "auth:Login", sourceCode, embedding, metadata)

    // Hybrid search limited to a specific topic
    results, _ := s.Find().
        InTopic(topicID).
        SimilarTo(embedding).
        Limit(5).
        Execute()

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
│  │  Circuit/GC/Events   │  Dual-DB atomicity                  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Dual BadgerDB (Graph: async writes + Dict: sync writes)   │ │
│  │  + Vector Store (sync writes, LSM-keyed)                   │ │
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
Prefix │ Index │ Key Size │ Value Size │ ID Layout (64-bit)
───────┼───────┼──────────┼────────────┼───────────────────────────────────
0x20   │ SPO   │ 25 bytes │ 16 bytes   │ [Topic:24] | [Local:40]
0x21   │ OPS   │ 25 bytes │ 16 bytes   │ [Topic:24] | [Local:40]
0x10   │ Chunk │ 9 bytes  │ S2 blob    │ [Local:40]
0xFF   │ Sys   │ 2 bytes  │ counter    │ —
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
Per block: [scale:4B][zero:4B][q_0:1B]...[q_31:1B]
  ↓
Compressed: 2,560 bytes (8-bit) or 1,536 bytes (4-bit)
```

| BitWidth | Compressed Size | Compression | RAM for 1M vectors |
|----------|----------------|-------------|---------------------|
| 8-bit | 2,560 bytes | 2.4x | 2.56 GB |
| 4-bit | 1,536 bytes | 4.0x | 1.54 GB |
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

## Package Structure

```
meb/
├── keys/              # 25-byte triple key encoding (TopicID packing)
├── dict/              # String interning (thread-safe LRU + sharded allocator)
├── store/             # BadgerDB config with deployment profiles
├── vector/            # Hybrid vector compression and search
│   ├── turboquant.go  # FWHT + block-wise 4/8-bit quantization
│   ├── partitioned.go # PartitionedRegistry (sharded by TopicID)
│   ├── registry.go    # Badger-native store + mmap cache, RCU revMap
│   ├── search.go      # Dual-path: mmap parallel + Badger streaming
│   └── math.go        # L2 normalize, dot product
├── circuit/           # Query timeout circuit breaker with state callbacks
├── utils/             # Zero-copy string/byte conversion
├── adapter/           # Mangle Datalog integration
├── store.go           # MEBStore orchestrator
├── knowledge_store.go # SPO/OPS dual-index write, orphan cleanup
├── scan.go            # Index selection scan (iter.Seq2 streaming)
├── content.go         # S2-compressed content storage
├── query_builder.go   # Neuro-symbolic query builder with LFTJ joins
├── telemetry.go       # Push telemetry (TelemetrySink interface)
├── wal.go             # Write-ahead log (mutex-free reads)
└── fact_store.go      # factstore.FactStore implementation
```

## Configuration

```go
// Default (Ingest-Heavy)
cfg := store.DefaultConfig("./data")

// Cloud Run optimized
cfg := store.SafeServingConfig("./data")

// Read-only
cfg := store.ReadOnlyConfig("./data")

// Enable verbose debug logging
cfg.Verbose = true

// Enable sharded dictionary (must be power of 2)
cfg.NumDictShards = 4
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
//   "wal_clear_failed" — WAL consistency issue
```

## Performance

**Benchmark results** (AMD Ryzen 9 5900HS with Radeon Graphics, in-memory BadgerDB, Go 1.23):

| Benchmark | Ops/sec | Latency | Memory | Allocs |
|-----------|---------|---------|--------|--------|
| **Vector Add** (1536-d, 8-bit Hybrid) | 27,866 | 61.3 µs/op | 145 KB/op | 39 |
| **Vector Search** (10K vectors, k=10) | 9,481 | 124.5 µs/op | 13.8 KB/op | 30 |
| **Fact Insertion** (single) | 101,677 | 9.8 µs/op | 2.8 KB/op | 62 |
| **Fact Insertion** (batch × 100) | 8,613 | 177.6 µs/op | 98.5 KB/op | 1,900 |
| **Scan** (10K facts, subject prefix) | 12,144 | 97.9 µs/op | 28.0 KB/op | 669 |
| **Scan Key-Only** (100K facts, subject prefix) | 1,213 | 1.05 ms/op | 28.0 KB/op | 669 |
| **Document Add** (content + vector + metadata) | 23,964 | 50.1 µs/op | 34.7 KB/op | 156 |
| **Hybrid Search** (1K docs, k=10) | 30,639 | 37.9 µs/op | 33.1 KB/op | 175 |
| **Dictionary Lookup** (GetOrCreate) | 1,619,012 | 707 ns/op | 400 B/op | 7 |
| **Delete Facts by Subject** (100 facts × 100 subjects) | 1,632 | 651 µs/op | 2.2 KB/op | 48 |

**Derived throughput:**

| Metric | Value | Notes |
|--------|-------|-------|
| **Vector Ingestion** | ~28K vectors/sec | Includes FWHT + quantization + Badger write |
| **Vector Search** | ~76M vectors/sec | Badger iterator streaming over 10K vectors |
| **Fact Ingestion** | ~102K facts/sec (single), ~861K facts/sec (batch) | Dual-index SPO+OPS write |
| **Scan Throughput** | ~102M keys/sec | Key-only, SPO prefix scan |
| **Dictionary Lookup** | ~1.6M lookups/sec | Sharded LRU cache hit |

**Key observations:**
- Batch insertion is ~8.5x more efficient per-fact than single insertion
- Scan latency scales with matching facts, not total graph size (prefix scan)
- Dictionary lookups are sub-microsecond with thread-safe LRU cache
- DeleteFactsBySubject uses scoped prefix scans (not full graph scan) for orphan cleanup

| Metric | Value | Notes |
|--------|-------|-------|
| **RAM Density** | Up to 1.2M nodes (1536-d) | Within 2GB RAM using Hybrid 4-bit |
| **Disk-Scaled** | Unlimited vectors | Badger-backed storage — not RAM-limited |
| **Cold Start** | < 200ms warm-up | Safe-Serving profile |
| **Join Latency** | Sub-2s | Complex code-graph traversals with circuit breaker |
| **Vector Search (hot)** | ~500K vectors/sec | mmap parallel scan, 4-way workers |
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
| **Recall@10** | `TestRecallAtK` (10K vectors, 10 seeds) | **90.0%** avg | > 80% | ✅ |
| **FWHT Invariance** | `TestFWHTInvariance` | Max error < 1e-5 (dims 4-2048) | FWHT(FWHT(v))/N == v | ✅ |
| **Energy Spreading** | `TestQuantizationDistribution` | CV: 0.29 → **0.16** (46% reduction) | avgCVFWHT < avgCVNoFWHT | ✅ |
| **8-bit Lossless** | `Test8BitLosslessVerification` | Max error: **0.063**, BER: **29.8%** | Max error < 1.0, BER < 50% | ✅ |

**Key observations:**
- **8-bit is near-lossless** (cosine = 1.0000) — 256 levels are more than enough for 32-element blocks
- **4-bit fidelity is excellent** (cosine = 0.9977) — far exceeds the 0.90 target even with spikes
- **Recall@10 = 90.0%** — tested on 10K vectors across 10 seeds with Gaussian + spike data
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

## Build

```bash
go build ./...
go test ./...
go vet ./...
```

## Dependencies

- `github.com/dgraph-io/badger/v4` — Key-value storage
- `codeberg.org/TauCeti/mangle-go` — Datalog reasoning engine
- `github.com/klauspost/compress` — S2 compression
- `github.com/hashicorp/golang-lru/v2` — Dictionary caching
- `golang.org/x/sys` — CPU feature detection (AVX2 dispatch)
