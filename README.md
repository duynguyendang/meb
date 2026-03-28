# meb

Mangle Extension for Badger — an embedded knowledge graph database combining triple-store semantics with TurboQuant vector search.

## Features

- **Triple Store**: Subject-Predicate-Object with dual SPO/OPS indexing (25-byte keys)
- **Multi-Topic Isolation**: 24-bit TopicID bit-packing enables 16M namespaces without a Graph column
- **TurboQuant Vectors**: Blockwise 4-bit/8-bit compression preserving full 1536 dimensions
- **Zero-Copy Streaming**: Go 1.23+ `iter.Seq2` for constant-memory scan operations
- **Dual BadgerDB**: Separate graph and dictionary databases for performance isolation
- **Datalog Integration**: Mangle `factstore.FactStore` interface for symbolic reasoning
- **Neuro-Symbolic Search**: Hybrid vector + graph query builder
- **Circuit Breaker**: Configurable query timeout protection
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

    // Add document with explicit topic isolation (Subject ID v2)
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
│  │  (SPO/OPS dual idx)   (S2 blobs)        (TQ compressed)   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Query Builder         │  Mangle Adapter                   │ │
│  │  (Neuro-Symbolic)      │  (Datalog reasoning)              │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Dual BadgerDB (Graph: async writes + Dict: sync writes)   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Hybrid Retrieval Strategy

MEB's Neuro-Symbolic pipeline combines neural vector search with symbolic graph traversal:

```
1. Semantic Seed (Neuro)
   ┌─────────────────────────────────────────────────────────┐
   │ Parallel TurboQuant scan in RAM finds top-K candidate   │
   │ IDs based on 1536-d vector similarity.                  │
   │ Constant memory via iter.Seq2 streaming.                │
   └────────────────────────┬────────────────────────────────┘
                            │ candidate IDs
                            ▼
2. Structural Expansion (Symbolic)
   ┌─────────────────────────────────────────────────────────┐
   │ Datalog/LFTJ engine uses seeds to traverse SPO/OPS      │
   │ indices for relational context (calls, imports, etc.)   │
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

### TurboQuant Vector Compression

Blockwise quantization preserving all 1536 dimensions (no MRL truncation):

```
Input: 1536-d float32 (OpenAI)
  ↓
Blockwise Quantize: Divide into 48 blocks of 32 elements
  ↓
Per block: [scale:4B][zero:4B][q_0:1B]...[q_31:1B]
  ↓
Compressed: 1,920 bytes (8-bit) or 1,152 bytes (4-bit)
```

| BitWidth | Compressed Size | Compression | RAM for 1M vectors |
|----------|----------------|-------------|---------------------|
| 8-bit | 1,920 bytes | 3.2x | 1.92 GB |
| 4-bit | 1,152 bytes | 5.3x | 1.15 GB |
| float32 | 6,144 bytes | 1x | 6.14 GB |

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
├── dict/              # String interning (LRU + BadgerDB)
├── store/             # BadgerDB config with deployment profiles
├── vector/            # TurboQuant compression and search
│   ├── turboquant.go  # Blockwise 4/8-bit quantization
│   ├── registry.go    # Vector storage and snapshot
│   ├── search.go      # Parallel TQ search
│   └── math.go        # L2 normalize, dot product
├── circuit/           # Query timeout circuit breaker
├── utils/             # Zero-copy string/byte conversion
├── adapter/           # Mangle Datalog integration
├── store.go           # MEBStore orchestrator
├── knowledge_store.go # SPO/OPS dual-index write
├── scan.go            # Index selection scan (iter.Seq2 streaming)
├── content.go         # S2-compressed content storage
├── query_builder.go   # Neuro-symbolic query builder
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
```

## Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **RAM Density** | Up to 1.2M nodes (1536-d) | Within 2GB RAM using TQ 4-bit |
| **Cold Start** | < 200ms warm-up | Safe-Serving profile |
| **Join Latency** | Sub-2s | Complex code-graph traversals with circuit breaker |
| **Vector Search** | ~500K vectors/sec | TQ 8-bit, 1536-dim, blockwise dot product |
| **Fact Insertion** | ~100K facts/sec | Batched dual-index write |
| **Scan Throughput** | ~20M keys/sec | Key-only, SPO/OPS prefix scan |
| **Content Read** | ~500MB/s | S2 decompression |

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

## Documentation

- [CSD (Conceptual Solution Design)](../kronos-docs/meb/CSD.md)

## Dependencies

- `github.com/dgraph-io/badger/v4` — Key-value storage
- `codeberg.org/TauCeti/mangle-go` — Datalog reasoning engine
- `github.com/klauspost/compress` — S2 compression
- `github.com/hashicorp/golang-lru/v2` — Dictionary caching
