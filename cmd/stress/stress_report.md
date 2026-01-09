# MEB Engine Stress Test Report

**Date:** 2026-01-09 12:30:13
**Hardware:** linux / amd64 / 16 Cores

## 1. Database Statistics
* **Total Documents:** 1000
* **Total Facts:** 3518
* **Database Size on Disk:** 0.63 GB
* **Peak RAM Usage:** 0.00 MB

## 2. Ingestion Performance
* **Total Time:** 283ms
* **Throughput:** 3535 docs/s
* **Fact Throughput:** 12434 facts/s

## 3. Query Performance (100 samples)

| Query Type | P50 (ms) | P95 (ms) | P99 (ms) | Ops/sec |
| :--- | :--- | :--- | :--- | :--- |
| **Vector Search (SQ8)** | 0.05 | 0.09 | 0.13 | 20408 |
| **Graph Scan (Quad)** | 0.02 | 0.04 | 0.07 | 55556 |
| **Metadata Lookup** | 0.10 | 0.10 | 0.10 | 10000 |
| **Mixed Query (RAG)** | 0.05 | 0.30 | 108.66 | 20833 |

## 4. Observations
* **Vector Search:** Excellent speed (P95 < 10ms)
* **Graph Scan:** Excellent speed (P95 < 1ms)
* **Metadata Lookup:** Excellent speed (P95 < 1ms)
* **Ingestion:** Acceptable throughput (>1K docs/sec)

---

**Test Configuration:**
* Vector Dimension: 64 (MRL)
* Content Size: ~500 bytes per document
* Graph Topology: Random "follows/references" links
* Batch Size: 100 documents

**Notes:**
* P50/P95/P99: 50th/95th/99th percentile latencies (lower is better)
* Ops/sec: Estimated operations per second based on P50 latency
* SQ8: Scalar Quantization (int8) for vector search
* S2: S2 compression for document content
