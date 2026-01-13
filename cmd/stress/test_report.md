# MEB Engine Stress Test Report

**Date:** 2026-01-13 13:38:38
**Hardware:** linux / amd64 / 4 Cores

## 1. Database Statistics
* **Total Documents:** 50000
* **Total Facts:** 175001
* **Database Size on Disk:** 2.32 GB
* **Peak RAM Usage:** 1161.39 MB

## 2. Ingestion Performance
* **Total Time:** 1m27.879s
* **Throughput:** 569 docs/s
* **Fact Throughput:** 1991 facts/s

## 3. Query Performance (10000 samples)

| Query Type | P50 (ms) | P95 (ms) | P99 (ms) | Ops/sec |
| :--- | :--- | :--- | :--- | :--- |
| **Vector Search (SQ8)** | 0.34 | 0.59 | 0.94 | 2907 |
| **Graph Scan (Quad)** | 0.01 | 0.02 | 0.03 | 142857 |
| **Metadata Lookup** | 0.01 | 0.01 | 0.03 | 200000 |
| **Mixed Query (RAG)** | 0.74 | 3.92 | 23.52 | 1344 |

## 4. Observations
* **Vector Search:** Excellent speed (P95 < 10ms)
* **Graph Scan:** Excellent speed (P95 < 1ms)
* **Metadata Lookup:** Excellent speed (P95 < 1ms)
* **Ingestion:** Consider optimization (569 docs/sec)
* **Memory Efficiency:** Acceptable (24356 bytes/doc)


---

**Test Configuration:**
* Vector Dimension: 64 (MRL)
* Content Size: ~500 bytes per document
* Graph Topology: Random "follows/references" links
* Batch Size: 1000 documents

**Notes:**
* P50/P95/P99: 50th/95th/99th percentile latencies (lower is better)
* Ops/sec: Estimated operations per second based on P50 latency
* SQ8: Scalar Quantization (int8) for vector search
* S2: S2 compression for document content
