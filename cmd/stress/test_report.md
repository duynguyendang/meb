# MEB Engine Stress Test Report

**Date:** 2026-01-13 09:39:30
**Hardware:** linux / amd64 / 16 Cores

## 1. Database Statistics
* **Total Documents:** 50000
* **Total Facts:** 175595
* **Database Size on Disk:** 0.85 GB
* **Peak RAM Usage:** 985.14 MB

## 2. Ingestion Performance
* **Total Time:** 48.586s
* **Throughput:** 1029 docs/s
* **Fact Throughput:** 3614 facts/s

## 3. Query Performance (100 samples)

| Query Type | P50 (ms) | P95 (ms) | P99 (ms) | Ops/sec |
| :--- | :--- | :--- | :--- | :--- |
| **Vector Search (SQ8)** | 0.30 | 0.38 | 0.44 | 3344 |
| **Graph Scan (Quad)** | 0.01 | 0.01 | 0.11 | 111111 |
| **Metadata Lookup** | 0.01 | 0.01 | 0.03 | 125000 |
| **Mixed Query (RAG)** | 0.36 | 1.11 | 103.29 | 2786 |

## 4. Observations
* **Vector Search:** Excellent speed (P95 < 10ms)
* **Graph Scan:** Excellent speed (P95 < 1ms)
* **Metadata Lookup:** Excellent speed (P95 < 1ms)
* **Ingestion:** Acceptable throughput (>1K docs/sec)
* **Memory Efficiency:** Acceptable (20660 bytes/doc)


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
