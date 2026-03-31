package vector

import (
	"fmt"
	"iter"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// PartitionedRegistry shards vectors by TopicID for distributed scale.
// Each topic gets its own independent VectorRegistry with separate mmap segments.
// Searches can be scoped to a single topic or broadcast across all topics.
type PartitionedRegistry struct {
	db     *badger.DB
	config *Config

	mu         sync.RWMutex
	partitions map[uint32]*VectorRegistry
}

// NewPartitionedRegistry creates a sharded vector registry.
// Each topic ID maps to an independent VectorRegistry with its own mmap segments.
func NewPartitionedRegistry(db *badger.DB, cfg *Config) *PartitionedRegistry {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &PartitionedRegistry{
		db:         db,
		config:     cfg,
		partitions: make(map[uint32]*VectorRegistry),
	}
}

// GetPartition returns the VectorRegistry for a specific topic.
// Creates a new partition if it doesn't exist.
func (p *PartitionedRegistry) GetPartition(topicID uint32) *VectorRegistry {
	p.mu.RLock()
	part, ok := p.partitions[topicID]
	p.mu.RUnlock()
	if ok {
		return part
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if part, ok := p.partitions[topicID]; ok {
		return part
	}

	part = NewRegistry(p.db, p.config)
	p.partitions[topicID] = part
	return part
}

// Add adds a vector to the partition for the given topicID.
func (p *PartitionedRegistry) Add(topicID uint32, id uint64, vec []float32) error {
	part := p.GetPartition(topicID)
	return part.Add(id, vec)
}

// AddWithHash adds a vector with semantic hash to the partition for the given topicID.
func (p *PartitionedRegistry) AddWithHash(topicID uint32, id uint64, vec []float32, hash uint8) error {
	part := p.GetPartition(topicID)
	return part.AddWithHash(id, vec, hash)
}

// Delete removes a vector from the partition for the given topicID.
func (p *PartitionedRegistry) Delete(topicID uint32, id uint64) bool {
	p.mu.RLock()
	part, ok := p.partitions[topicID]
	p.mu.RUnlock()
	if !ok {
		return false
	}
	return part.Delete(id)
}

// HasVector checks if a vector exists in the partition for the given topicID.
func (p *PartitionedRegistry) HasVector(topicID uint32, id uint64) bool {
	p.mu.RLock()
	part, ok := p.partitions[topicID]
	p.mu.RUnlock()
	if !ok {
		return false
	}
	return part.HasVector(id)
}

// Search searches vectors in a specific topic partition.
func (p *PartitionedRegistry) Search(topicID uint32, queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	p.mu.RLock()
	part, ok := p.partitions[topicID]
	p.mu.RUnlock()
	if !ok {
		return func(yield func(SearchResult, error) bool) {}
	}
	return part.Search(queryVec, k)
}

// SearchInTopic is an alias for Search (topic is already specified).
func (p *PartitionedRegistry) SearchInTopic(topicID uint32, queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return p.Search(topicID, queryVec, k)
}

// SearchAllTopics searches across all topic partitions in parallel and merges results.
// Returns top-k results across all partitions.
func (p *PartitionedRegistry) SearchAllTopics(queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		p.mu.RLock()
		partitions := make([]*VectorRegistry, 0, len(p.partitions))
		for _, part := range p.partitions {
			partitions = append(partitions, part)
		}
		p.mu.RUnlock()

		if len(partitions) == 0 {
			return
		}

		if len(partitions) == 1 {
			for result, err := range partitions[0].Search(queryVec, k) {
				if err != nil {
					yield(SearchResult{}, err)
					return
				}
				if !yield(result, nil) {
					return
				}
			}
			return
		}

		// Search all partitions in parallel
		type partitionResult struct {
			results []SearchResult
			err     error
		}

		ch := make(chan partitionResult, len(partitions))
		for _, part := range partitions {
			go func(p *VectorRegistry) {
				results, err := collectSearchResults(p.Search(queryVec, k))
				ch <- partitionResult{results: results, err: err}
			}(part)
		}

		// Merge results
		allResults := make([]SearchResult, 0, k*len(partitions))
		for i := 0; i < len(partitions); i++ {
			pr := <-ch
			if pr.err != nil {
				yield(SearchResult{}, pr.err)
				return
			}
			allResults = append(allResults, pr.results...)
		}

		// Sort by score descending and return top-k
		sort.Slice(allResults, func(i, j int) bool {
			return allResults[i].Score > allResults[j].Score
		})

		limit := k
		if limit > len(allResults) {
			limit = len(allResults)
		}

		for i := 0; i < limit; i++ {
			if !yield(allResults[i], nil) {
				return
			}
		}
	}
}

// Count returns the total number of vectors across all partitions.
func (p *PartitionedRegistry) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	total := 0
	for _, part := range p.partitions {
		total += part.Count()
	}
	return total
}

// CountByTopic returns the number of vectors in a specific topic partition.
func (p *PartitionedRegistry) CountByTopic(topicID uint32) int {
	p.mu.RLock()
	part, ok := p.partitions[topicID]
	p.mu.RUnlock()
	if !ok {
		return 0
	}
	return part.Count()
}

// TopicIDs returns all topic IDs that have vectors.
func (p *PartitionedRegistry) TopicIDs() []uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]uint32, 0, len(p.partitions))
	for id := range p.partitions {
		ids = append(ids, id)
	}
	return ids
}

// LoadSnapshot loads snapshots for all existing partitions.
func (p *PartitionedRegistry) LoadSnapshot() error {
	p.mu.RLock()
	partitions := make([]*VectorRegistry, 0, len(p.partitions))
	for _, part := range p.partitions {
		partitions = append(partitions, part)
	}
	p.mu.RUnlock()

	for _, part := range partitions {
		if err := part.LoadSnapshot(); err != nil {
			return fmt.Errorf("failed to load snapshot for partition: %w", err)
		}
	}
	return nil
}

// SaveSnapshot saves snapshots for all partitions.
func (p *PartitionedRegistry) SaveSnapshot() error {
	p.mu.RLock()
	partitions := make([]*VectorRegistry, 0, len(p.partitions))
	for _, part := range p.partitions {
		partitions = append(partitions, part)
	}
	p.mu.RUnlock()

	for _, part := range partitions {
		if err := part.SaveSnapshot(); err != nil {
			return fmt.Errorf("failed to save snapshot for partition: %w", err)
		}
	}
	return nil
}

// Close closes all partitions.
func (p *PartitionedRegistry) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for _, part := range p.partitions {
		if err := part.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.partitions = make(map[uint32]*VectorRegistry)
	return firstErr
}

// HybridConfig returns the hybrid config (same for all partitions).
func (p *PartitionedRegistry) HybridConfig() *HybridConfig {
	return p.config.HybridConfig()
}

// VectorSize returns the vector size (same for all partitions).
func (p *PartitionedRegistry) VectorSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, part := range p.partitions {
		return part.VectorSize()
	}
	// If no partitions yet, compute from config
	return HybridVectorSize(p.config.FullDim, p.config.HybridConfig()) + hashSize
}

func collectSearchResults(seq iter.Seq2[SearchResult, error]) ([]SearchResult, error) {
	var results []SearchResult
	for r, err := range seq {
		if err != nil {
			return results, err
		}
		results = append(results, r)
	}
	return results, nil
}
