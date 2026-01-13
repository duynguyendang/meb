# MEB Engine Stress Test Report

**Date:** 2026-01-13 13:07:56
**Hardware:** linux / amd64 / 4 Cores

## 1. Database Statistics
* **Total Documents:** 10000
* **Total Facts:** 34868
* **Database Size on Disk:** 0.22 GB
* **Peak RAM Usage:** 457.73 MB

## 2. Ingestion Performance
* **Total Time:** 15.625s
* **Throughput:** 640 docs/s
* **Fact Throughput:** 2232 facts/s

## 3. Query Performance (10000 samples)

| Query Type | P50 (ms) | P95 (ms) | P99 (ms) | Ops/sec |
| :--- | :--- | :--- | :--- | :--- |
| **Vector Search (SQ8)** | 0.12 | 0.20 | 0.25 | 8065 |
| **Graph Scan (Quad)** | 0.01 | 0.02 | 0.03 | 142857 |
| **Metadata Lookup** | 0.01 | 0.01 | 0.02 | 200000 |
| **Mixed Query (RAG)** | 0.18 | 2.03 | 29.99 | 5464 |

## 4. Observations
* **Vector Search:** Excellent speed (P95 < 10ms)
* **Graph Scan:** Excellent speed (P95 < 1ms)
* **Metadata Lookup:** Excellent speed (P95 < 1ms)
* **Ingestion:** Consider optimization (640 docs/sec)
* **Memory Efficiency:** Acceptable (47997 bytes/doc)


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
