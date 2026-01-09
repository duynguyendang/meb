package main

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// StatsCollector tracks memory usage during the stress test
type StatsCollector struct {
	peakRAM    uint64
	stopChan   chan struct{}
	wg         sync.WaitGroup
	mu         sync.Mutex
}

// NewStatsCollector creates a new stats collector
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		stopChan: make(chan struct{}),
	}
}

// Start begins monitoring memory usage
func (sc *StatsCollector) Start() {
	sc.wg.Add(1)
	go func() {
		defer sc.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				sc.mu.Lock()
				if m.Alloc > sc.peakRAM {
					sc.peakRAM = m.Alloc
				}
				sc.mu.Unlock()
			case <-sc.stopChan:
				return
			}
		}
	}()
}

// Stop stops monitoring memory usage
func (sc *StatsCollector) Stop() {
	close(sc.stopChan)
	sc.wg.Wait()
}

// GetPeakRAM returns the peak RAM usage in bytes
func (sc *StatsCollector) GetPeakRAM() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.peakRAM
}

// generateReport creates a markdown report with the test results
func generateReport(stats *Stats) error {
	content := fmt.Sprintf(`# MEB Engine Stress Test Report

**Date:** %s
**Hardware:** %s / %s / %d Cores

## 1. Database Statistics
* **Total Documents:** %d
* **Total Facts:** %d
* **Database Size on Disk:** %.2f GB
* **Peak RAM Usage:** %.2f MB

## 2. Ingestion Performance
* **Total Time:** %s
* **Throughput:** %.0f docs/s
* **Fact Throughput:** %.0f facts/s

## 3. Query Performance (%d samples)

| Query Type | P50 (ms) | P95 (ms) | P99 (ms) | Ops/sec |
| :--- | :--- | :--- | :--- | :--- |
| **Vector Search (SQ8)** | %.2f | %.2f | %.2f | %.0f |
| **Graph Scan (Quad)** | %.2f | %.2f | %.2f | %.0f |
| **Metadata Lookup** | %.2f | %.2f | %.2f | %.0f |
| **Mixed Query (RAG)** | %.2f | %.2f | %.2f | %.0f |

## 4. Observations
%s

---

**Test Configuration:**
* Vector Dimension: 64 (MRL)
* Content Size: ~500 bytes per document
* Graph Topology: Random "follows/references" links
* Batch Size: %d documents

**Notes:**
* P50/P95/P99: 50th/95th/99th percentile latencies (lower is better)
* Ops/sec: Estimated operations per second based on P50 latency
* SQ8: Scalar Quantization (int8) for vector search
* S2: S2 compression for document content
`,
		time.Now().Format("2006-01-02 15:04:05"),
		runtime.GOOS,
		runtime.GOARCH,
		runtime.NumCPU(),
		stats.TotalDocuments,
		int(stats.TotalFacts),
		stats.DatabaseSizeGB,
		float64(stats.PeakRAMBytes)/(1024*1024),
		stats.IngestionDuration.Round(time.Millisecond),
		stats.IngestionDocsPerSec,
		stats.IngestionFactsPerSec,
		*flagQuerySamples,
		stats.VectorSearchP50, stats.VectorSearchP95, stats.VectorSearchP99, stats.VectorSearchQPS,
		stats.GraphScanP50, stats.GraphScanP95, stats.GraphScanP99, stats.GraphScanQPS,
		stats.ContentFetchP50, stats.ContentFetchP95, stats.ContentFetchP99, stats.ContentFetchQPS,
		stats.MixedQueryP50, stats.MixedQueryP95, stats.MixedQueryP99, stats.MixedQueryQPS,
		generateObservations(stats),
		*flagBatch,
	)

	return os.WriteFile(*flagReport, []byte(content), 0644)
}

// generateObservations creates auto-generated comments based on performance thresholds
func generateObservations(stats *Stats) string {
	var obs string

	// Vector search observations
	if stats.VectorSearchP95 < 10 {
		obs += "* **Vector Search:** Excellent speed (P95 < 10ms)\n"
	} else if stats.VectorSearchP95 < 50 {
		obs += "* **Vector Search:** Good performance (P95 < 50ms)\n"
	} else if stats.VectorSearchP95 < 100 {
		obs += "* **Vector Search:** Acceptable performance (P95 < 100ms)\n"
	} else {
		obs += fmt.Sprintf("* **Vector Search:** Consider optimization (P95 = %.2fms)\n", stats.VectorSearchP95)
	}

	// Graph scan observations
	if stats.GraphScanP95 < 1 {
		obs += "* **Graph Scan:** Excellent speed (P95 < 1ms)\n"
	} else if stats.GraphScanP95 < 5 {
		obs += "* **Graph Scan:** Good performance (P95 < 5ms)\n"
	} else if stats.GraphScanP95 < 10 {
		obs += "* **Graph Scan:** Acceptable performance (P95 < 10ms)\n"
	} else {
		obs += fmt.Sprintf("* **Graph Scan:** Consider optimization (P95 = %.2fms)\n", stats.GraphScanP95)
	}

	// Metadata lookup observations
	if stats.ContentFetchP95 < 1 {
		obs += "* **Metadata Lookup:** Excellent speed (P95 < 1ms)\n"
	} else if stats.ContentFetchP95 < 5 {
		obs += "* **Metadata Lookup:** Good performance (P95 < 5ms)\n"
	} else if stats.ContentFetchP95 < 10 {
		obs += "* **Metadata Lookup:** Acceptable performance (P95 < 10ms)\n"
	} else {
		obs += fmt.Sprintf("* **Metadata Lookup:** Consider optimization (P95 = %.2fms)\n", stats.ContentFetchP95)
	}

	// Ingestion observations
	if stats.IngestionDocsPerSec > 10000 {
		obs += "* **Ingestion:** Excellent throughput (>10K docs/sec)\n"
	} else if stats.IngestionDocsPerSec > 5000 {
		obs += "* **Ingestion:** Good throughput (>5K docs/sec)\n"
	} else if stats.IngestionDocsPerSec > 1000 {
		obs += "* **Ingestion:** Acceptable throughput (>1K docs/sec)\n"
	} else {
		obs += fmt.Sprintf("* **Ingestion:** Consider optimization (%.0f docs/sec)\n", stats.IngestionDocsPerSec)
	}

	// Memory efficiency observations
	bytesPerDoc := float64(stats.PeakRAMBytes) / float64(stats.TotalDocuments)
	if bytesPerDoc < 1000 {
		obs += fmt.Sprintf("* **Memory Efficiency:** Excellent (%.0f bytes/doc)\n", bytesPerDoc)
	} else if bytesPerDoc < 5000 {
		obs += fmt.Sprintf("* **Memory Efficiency:** Good (%.0f bytes/doc)\n", bytesPerDoc)
	} else {
		obs += fmt.Sprintf("* **Memory Efficiency:** Acceptable (%.0f bytes/doc)\n", bytesPerDoc)
	}

	return obs
}
