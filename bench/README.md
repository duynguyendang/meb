# MEB Benchmarks

## Running benchmarks

```bash
# Run all benchmarks (smoke)
go test -bench=. -benchtime=1x -run='^$' ./bench/...

# Run with full timing
go test -bench=. -benchtime=100x -run='^$' ./bench/...

# Run recall test
go test -run TestRecallAt10 ./bench/...

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
      "ops_per_sec": 500000.0,
      "p50_ms": 2.1,
      "p95_ms": 3.5,
      "p99_ms": 5.0
    }
  ]
}
```
