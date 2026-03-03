package vector

import (
	"math"
	"math/rand"
	"sort"
)

const (
	// PQ parameters
	PQSubBytes  = 8         // Bytes per sub-vector (dimensions = bytes since float32)
	PQNumSubs   = 8         // Number of sub-vectors (64 / 8 = 8)
	PQCodeSize  = PQNumSubs // Bytes per encoded vector (1 byte per sub-vector)
	PQKClusters = 256       // Number of centroids per sub-vector (2^8 = 256)

	// Hierarchical search thresholds
	CoarseThreshold     = 0.7 // MRL similarity threshold for early exit
	CandidateMultiplier = 10  // Fetch 10x candidates from coarse search
)

// PQCodebook stores the centroids for PQ encoding.
// Each sub-vector has its own codebook of 256 centroids.
type PQCodebook struct {
	// centroids[s][k] = centroid for sub-vector s, cluster k
	// Shape: [PQNumSubs][PQKClusters][PQSubBytes]float32
	centroids [][][]float32

	// Trained indicates if the codebook has been trained
	Trained bool
}

// NewPQCodebook creates a new PQ codebook structure
func NewPQCodebook() *PQCodebook {
	centroids := make([][][]float32, PQNumSubs)
	for s := 0; s < PQNumSubs; s++ {
		centroids[s] = make([][]float32, PQKClusters)
		for k := 0; k < PQKClusters; k++ {
			centroids[s][k] = make([]float32, PQSubBytes)
		}
	}
	return &PQCodebook{
		centroids: centroids,
		Trained:   false,
	}
}

// Train builds the PQ codebook from a set of vectors using k-means.
// This should be called after adding vectors but before encoding.
func (c *PQCodebook) Train(vectors [][]float32) {
	if len(vectors) < PQKClusters*10 {
		// Not enough vectors to train properly
		c.Trained = false
		return
	}

	rand.Seed(42) // Deterministic training

	// Initialize centroids using k-means++
	for s := 0; s < PQNumSubs; s++ {
		// Extract sub-vectors
		subVecs := extractSubVectors(vectors, s)

		// K-means++ initialization
		centroids := c.centroids[s]

		// Pick first centroid randomly
		idx := rand.Intn(len(subVecs))
		copy(centroids[0], subVecs[idx])

		// Pick remaining centroids with probability proportional to distance squared
		for k := 1; k < PQKClusters; k++ {
			distances := make([]float32, len(subVecs))
			var totalDist float32

			for i, sv := range subVecs {
				// Find distance to nearest centroid
				minDist := float32(math.MaxFloat32)
				for _, cent := range centroids[:k] {
					d := euclideanDistance(sv, cent)
					if d < minDist {
						minDist = d
					}
				}
				distances[i] = minDist * minDist
				totalDist += distances[i]
			}

			// Select next centroid
			r := float32(rand.Float64()) * totalDist
			sum := float32(0)
			for i, d := range distances {
				sum += d
				if sum >= r {
					copy(centroids[k], subVecs[i])
					break
				}
			}
		}

		// Run k-means iterations (10 is usually enough)
		for iter := 0; iter < 10; iter++ {
			// Assign vectors to nearest centroid
			assignments := make([]int, len(subVecs))
			clusterSums := make([][]float32, PQKClusters)
			clusterCounts := make([]int, PQKClusters)

			for k := 0; k < PQKClusters; k++ {
				clusterSums[k] = make([]float32, PQSubBytes)
			}

			for i, sv := range subVecs {
				minDist := float32(math.MaxFloat32)
				bestK := 0
				for k := 0; k < PQKClusters; k++ {
					d := euclideanDistance(sv, centroids[k])
					if d < minDist {
						minDist = d
						bestK = k
					}
				}
				assignments[i] = bestK
				clusterCounts[bestK]++
				for d := 0; d < PQSubBytes; d++ {
					clusterSums[bestK][d] += sv[d]
				}
			}

			// Update centroids
			for k := 0; k < PQKClusters; k++ {
				if clusterCounts[k] > 0 {
					for d := 0; d < PQSubBytes; d++ {
						centroids[k][d] = clusterSums[k][d] / float32(clusterCounts[k])
					}
				}
			}
		}
	}

	c.Trained = true
}

// Encode encodes a vector using the trained codebook.
// Returns PQCodeSize bytes (one byte per sub-vector).
func (c *PQCodebook) Encode(vec []float32) []byte {
	if !c.Trained {
		return nil
	}

	code := make([]byte, PQNumSubs)
	for s := 0; s < PQNumSubs; s++ {
		subVec := vec[s*PQSubBytes : (s+1)*PQSubBytes]
		centroids := c.centroids[s]

		minDist := float32(math.MaxFloat32)
		bestK := 0
		for k := 0; k < PQKClusters; k++ {
			d := euclideanDistance(subVec, centroids[k])
			if d < minDist {
				minDist = d
				bestK = k
			}
		}
		code[s] = byte(bestK)
	}
	return code
}

// Decode reconstructs an approximate vector from its PQ code.
// Returns a 64-d float32 vector.
func (c *PQCodebook) Decode(code []byte) []float32 {
	if !c.Trained || len(code) != PQNumSubs {
		return nil
	}

	vec := make([]float32, PQNumSubs*PQSubBytes)
	for s := 0; s < PQNumSubs; s++ {
		k := int(code[s])
		copy(vec[s*PQSubBytes:(s+1)*PQSubBytes], c.centroids[s][k])
	}
	return vec
}

// ComputeReconstructedDistance computes the distance between a query and a PQ-encoded vector.
// This is an approximation - exact distance would require decoding.
func (c *PQCodebook) ComputeReconstructedDistance(query []float32, code []byte) float32 {
	if !c.Trained || len(code) != PQNumSubs {
		return 0
	}

	var totalDist float32
	for s := 0; s < PQNumSubs; s++ {
		subQuery := query[s*PQSubBytes : (s+1)*PQSubBytes]
		centroid := c.centroids[s][code[s]]
		totalDist += euclideanDistanceSquared(subQuery, centroid)
	}
	return float32(math.Sqrt(float64(totalDist)))
}

// extractSubVectors extracts a specific sub-vector from all vectors
func extractSubVectors(vectors [][]float32, subIdx int) [][]float32 {
	subVecs := make([][]float32, len(vectors))
	for i, vec := range vectors {
		subVecs[i] = make([]float32, PQSubBytes)
		start := subIdx * PQSubBytes
		copy(subVecs[i], vec[start:start+PQSubBytes])
	}
	return subVecs
}

func euclideanDistance(v1, v2 []float32) float32 {
	return float32(math.Sqrt(float64(euclideanDistanceSquared(v1, v2))))
}

func euclideanDistanceSquared(v1, v2 []float32) float32 {
	var sum float32
	for i := range v1 {
		d := v1[i] - v2[i]
		sum += d * d
	}
	return sum
}

// PQHybridSearch performs hierarchical search: coarse MRL -> fine PQ refinement
func (r *VectorRegistry) PQHybridSearch(queryVec []float32, k int) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

	r.mu.RLock()
	codebook := r.pqCodebook
	hasPQ := codebook != nil && codebook.Trained && len(r.pqData) > 0
	r.mu.RUnlock()

	if !hasPQ {
		// Fall back to regular MRL search
		return r.Search(queryVec, k)
	}

	// Step 1: Coarse search using MRL (64-d)
	candidateCount := k * CandidateMultiplier
	if candidateCount < 100 {
		candidateCount = 100
	}

	mrlQuery := ProcessMRL(queryVec)
	quantizedQuery := Quantize(mrlQuery)

	r.mu.RLock()
	numVectors := len(r.revMap)
	data := r.data
	r.mu.RUnlock()

	if numVectors == 0 {
		return nil, nil
	}

	// Get MRL candidates
	candidates := r.getMRLCandidates(data, quantizedQuery, candidateCount)
	if len(candidates) == 0 {
		return nil, nil
	}

	// Step 2: Early exit if top MRL score is above threshold
	if len(candidates) > 0 && candidates[0].score > CoarseThreshold {
		// Return top-k from MRL candidates directly
		if len(candidates) > k {
			candidates = candidates[:k]
		}
		results := make([]SearchResult, len(candidates))
		r.mu.RLock()
		for i, c := range candidates {
			results[i] = SearchResult{ID: r.revMap[c.idx], Score: c.score}
		}
		r.mu.RUnlock()
		return results, nil
	}

	// Step 3: Refine candidates using PQ distance
	type refinedResult struct {
		idx   int
		score float32
	}

	refined := make([]refinedResult, 0, len(candidates))
	r.mu.RLock()
	for _, c := range candidates {
		// Check bounds (safe unless data is completely out of sync, which lock prevents)
		if c.idx*PQCodeSize >= len(r.pqData) {
			refined = append(refined, refinedResult{idx: c.idx, score: c.score})
			continue
		}

		pqCode := r.pqData[c.idx*PQCodeSize : (c.idx+1)*PQCodeSize]

		// Compute PQ approximate distance (lower is better, convert to similarity)
		pqDist := codebook.ComputeReconstructedDistance(mrlQuery, pqCode)
		// Convert distance to similarity score
		pqSim := 1.0 / (1.0 + pqDist)
		refined = append(refined, refinedResult{idx: c.idx, score: pqSim})
	}
	r.mu.RUnlock()

	// Sort by refined score (descending)
	sort.Slice(refined, func(i, j int) bool {
		return refined[i].score > refined[j].score
	})

	// Return top-k
	results := make([]SearchResult, 0, k)
	r.mu.RLock()
	for i := 0; i < len(refined) && i < k; i++ {
		results = append(results, SearchResult{
			ID:    r.revMap[refined[i].idx],
			Score: refined[i].score,
		})
	}
	r.mu.RUnlock()

	return results, nil
}

// getMRLCandidates returns top-k candidates based on MRL int8 dot product
func (r *VectorRegistry) getMRLCandidates(data []int8, query []int8, k int) []scoreIndex {
	type candidate struct {
		idx   int
		score float32
	}

	// Simple linear scan for candidates
	h := make([]candidate, 0, k)

	numVectors := len(data) / MRLDim

	for idx := 0; idx < numVectors; idx++ {
		offset := idx * MRLDim
		score := DotProductInt8(data[offset:offset+MRLDim], query)

		if len(h) < k {
			h = append(h, candidate{idx: idx, score: score})
		} else if score > h[0].score {
			h[0] = candidate{idx: idx, score: score}
		}
	}

	// Convert to scoreIndex and sort
	result := make([]scoreIndex, len(h))
	for i, c := range h {
		result[i] = scoreIndex{score: c.score, idx: c.idx}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].score > result[j].score
	})

	return result
}
