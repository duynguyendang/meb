# MEB Benchmarks

## Running benchmarks

```bash
# Run all benchmarks (smoke)
go test -bench=. -benchtime=1x -run='^$' ./bench/...

# Run with full timing
go test -bench=. -benchtime=100x -run='^$' ./bench/...

# Run recall tests
go test -run "TestHNSWRecallAt10|TestIVFPQRecallAt10" -v ./bench/...

# Run vector-level benchmarks (pruning stats, ADC, etc.)
go test -run "^$" -bench "BenchmarkPruning|BenchmarkADC" -benchmem ./vector/...

# Run CLI benchmark tool (writes JSON report to stdout)
go run ./cmd/bench
```

## Benchmark scenarios

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkVectorSearch_1K` | Search 1K vectors, top-10 |
| `BenchmarkVectorSearch_10K` | Search 10K vectors, top-10 |
| `BenchmarkVectorSearch_100K` | Search 100K vectors, top-10 |
| `BenchmarkVectorAdd_Sustained` | Ingest throughput |
| `BenchmarkFactInsertion_Single` | Single fact insert throughput |
| `BenchmarkFactInsertion_Batch100` | Batch (100) fact insert throughput |
| `BenchmarkLFTJ_3Atom` | 3-atom multi-join latency |
| `BenchmarkLFTJ_5Atom` | 5-atom multi-join latency |

## ANN recall tests (bench/ann_bench_test.go)

| Test | Status | Target |
|------|--------|--------|
| `TestHNSWRecallAt10` | **PASS** (0.92–0.94) | recall@10 ≥ 0.85 |
| `TestHNSWInsertThroughput` | **PASS** (~450 vec/s for dim=128) | reports throughput |
| `TestHNSWGraphConnectivity` | **PASS** | diagnostic: verifies graph structure |
| `TestIVFPQRecallAt10` | **SKIP** (PQ codebook training bug) | recall@10 ≥ 0.80 |
| `TestIVFPQTrainTime` | **PASS** (~116s for 10K vecs) | reports train time |

## Vector-level benchmarks (vector/)

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkPruningStats/thr_X` | Cauchy-Schwarz pruning: blocks evaluated per vector at various thresholds |
| `BenchmarkPruningSkippedRatio` | Pruning skip ratio with a realistic top-k threshold |
| `BenchmarkADCVSScalar/*` | ADC scalar vs platform-optimized throughput (GB/s) |
| `BenchmarkADCAccumScalar/*` | Scalar ADC accumulator alone |
| `BenchmarkADCAccumDispatched/*` | Dispatched (SSE/NEON) ADC accumulator alone |

## Known issues

- **IVF-PQ recall = 0**: The PQ codebook training (`trainPQCodebook` in `vector/ivfpq_train.go`)
  produces degenerate codes where all vectors yield identical ADC distances. The search returns
  results but with zero overlap against brute-force ground truth. This is a pre-existing bug
  in the mini-batch k-means codebook training, not in the benchmark test. The test is skipped
  until this is fixed.

## Full datasets

Full ann-benchmarks.com datasets (SIFT, GLOVE, Deep1B) are not committed to git.
Download them at benchmark time:

```bash
# Download SIFT-1M
make download-sift

# Run benchmarks with full SIFT dataset
go test -bench=B.*SIFT -benchtime=1x ./bench/...
```

## JSON report

The CLI tool (`go run ./cmd/bench`) produces a JSON report for regression tracking:

```json
{
  "timestamp": "2026-06-18T00:00:00Z",
  "scenarios": [
    {
      "name": "VectorSearch_1K",
      "path": "brute-force",
      "ops_per_sec": 500000.0,
      "p50_ms": 2.1,
      "p95_ms": 3.5,
      "p99_ms": 5.0
    }
  ]
}
```
