# MEB Roadmap

## Phase 1: Core Stability (In Progress)

- [x] Dual-indexing (SPOG, OPSG, GSPO)
- [x] Dictionary encoding with LRU cache
- [x] Vector search (MRL + SQ8)
- [x] Circuit breaker
- [x] Configuration profiles
- [x] Lock-free atomic counting
- [x] Graph caching

## Phase 2: Query Power (In Progress)

### 2.1 Rich Predicates (DONE)
- [x] Regex matching during scan: `ScanWithFilters("", "matches", "^user-\\d+$", "", filters)`
- [x] Range filtering: `ScanWithFilters("", "age", "", "", filters)` with `PredicateRange`
- [x] Comparison operators: GT, LT, GTE, LTE
- [x] Contains substring filter

### 2.2 Full Magic Sets Optimization
- [ ] Automatic query rewriting for recursive rules
- [ ] Binding propagation through rule bodies
- [ ] Specialized query plans per rule

## Phase 3: Vector Scale (Planned)

### 3.1 Matryoshka + PQ Hybrid Search
- [ ] Product Quantization (PQ) for 256-byte vectors
- [ ] Hierarchical search: coarse (MRL) → fine (PQ)
- [ ] Early-exit approximate matching

### 3.2 Portable Vector Index Snapshots
- [ ] Export HNSW graph + quantized vectors to portable format
- [ ] Versioned snapshots for A/B testing
- [ ] Cross-instance loading without rebuild

## Phase 4: Operational Reliability (Planned)

### 4.1 Incremental SST Watcher (CDC)
- [ ] Monitor LSM tree compaction events
- [ ] Expose new SST files via callback channel
- [ ] Export key ranges for incremental processing

### 4.2 Automated Value Log GC Policy
- [ ] Adaptive GC based on write patterns
- [ ] Low-traffic window scheduling
- [ ] GC metrics (`log_discard_ratio`, `rewrite_amplification`)

---

## Implementation Notes

### Rich Predicates (Phase 2.1)

Implementation approach:
1. Add `PredicateFunc` type to scan.go
2. Extend `scanOptions` with predicate filters
3. Evaluate predicates during iterator scan
4. Support regex via `regexp.Regexp` and ranges via numeric comparison

```go
type PredicateFilter struct {
    Type  string // "regex", "range", "gt", "lt", "eq"
    Value interface{}
}

func (m *MEBStore) ScanWithPredicates(s, p, o, g string, filters []PredicateFilter) iter.Seq2[Fact, error]
```

### Lock-free Counting (Already Done)

Current implementation uses `atomic.Uint64`:
```go
type MEBStore struct {
    numFacts atomic.Uint64
}
```
Sustains >1M facts/sec ingestion without blocking.

### Portable Snapshots (Phase 3.2)

Format:
```
snapshot_v1.tar
├── meta.json          # Version, dimensions, count
├── vectors.data      # Raw quantized vectors
├── ids.data          # ID mapping
├── graph.bin         # HNSW graph structure (optional)
└── manifest.sha256   # Integrity hash
```
