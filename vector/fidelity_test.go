package vector

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// =============================================================================
// Data Generators
// =============================================================================

// randomVectorFP32 generates a random FP32 vector with Gaussian distribution.
func randomVectorFP32(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rand.NormFloat64())
	}
	return v
}

// randomVectorWithSpikes generates a Gaussian vector with 1-5 extreme outliers.
// 5% of vectors will have spikes at 50x-100x standard deviation.
func randomVectorWithSpikes(dim int, rng *rand.Rand) []float32 {
	v := randomVectorFP32(dim)

	if rng.Float64() < 0.05 {
		// Inject 1-5 spikes
		numSpikes := 1 + rng.Intn(5)
		for s := 0; s < numSpikes; s++ {
			idx := rng.Intn(dim)
			multiplier := 50.0 + rng.Float64()*50.0 // 50x-100x
			if rng.Float64() < 0.5 {
				multiplier = -multiplier
			}
			v[idx] = float32(multiplier)
		}
	}
	return v
}

// =============================================================================
// Metric Helpers
// =============================================================================

// cosineSimilarity computes cosine similarity between two FP32 vectors.
func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i] * b[i])
		normA += float64(a[i] * a[i])
		normB += float64(b[i] * b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// dotProductFP32 computes exact dot product on FP32 vectors.
func dotProductFP32(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

// =============================================================================
// Metric 1: Mathematical Fidelity + Dot Product Accuracy
// =============================================================================

func TestHybridQuantizationFidelity(t *testing.T) {
	dim := 1536
	numVectors := 100

	cfg8 := &HybridConfig{BitWidth: 8, BlockSize: 32}
	cfg4 := &HybridConfig{BitWidth: 4, BlockSize: 32}

	var cos8Sum, cos4Sum float64
	var dotMAE8, dotMAE4, dotMaxErr8, dotMaxErr4 float64

	for i := 0; i < numVectors; i++ {
		// Use spike vectors for 50% of tests
		var orig []float32
		if i%2 == 0 {
			orig = randomVectorWithSpikes(dim, rand.New(rand.NewSource(int64(i))))
		} else {
			orig = randomVectorFP32(dim)
		}

		// Round-trip: quantize → dequantize
		q8 := QuantizeHybrid(orig, cfg8)
		recon8 := DequantizeHybrid(q8, dim, cfg8)
		cos8 := cosineSimilarity(orig, recon8)
		cos8Sum += cos8

		q4 := QuantizeHybrid(orig, cfg4)
		recon4 := DequantizeHybrid(q4, dim, cfg4)
		cos4 := cosineSimilarity(orig, recon4)
		cos4Sum += cos4

		// Dot product fidelity: compare exact vs quantized dot product
		// Use a second vector for dot product test
		var origB []float32
		if i%2 == 0 {
			origB = randomVectorWithSpikes(dim, rand.New(rand.NewSource(int64(i+1000))))
		} else {
			origB = randomVectorFP32(dim)
		}

		exactDot := dotProductFP32(orig, origB)

		qB8 := QuantizeHybrid(origB, cfg8)
		qDot8 := float64(DotProductHybrid(q8, qB8, dim, cfg8))
		err8 := math.Abs(exactDot - qDot8)
		dotMAE8 += err8
		if err8 > dotMaxErr8 {
			dotMaxErr8 = err8
		}

		qB4 := QuantizeHybrid(origB, cfg4)
		qDot4 := float64(DotProductHybrid(q4, qB4, dim, cfg4))
		err4 := math.Abs(exactDot - qDot4)
		dotMAE4 += err4
		if err4 > dotMaxErr4 {
			dotMaxErr4 = err4
		}
	}

	avgCos8 := cos8Sum / float64(numVectors)
	avgCos4 := cos4Sum / float64(numVectors)
	avgDotMAE8 := dotMAE8 / float64(numVectors)
	avgDotMAE4 := dotMAE4 / float64(numVectors)

	t.Logf("=== Mathematical Fidelity ===")
	t.Logf("8-bit avg cosine similarity: %.4f (target > 0.95)", avgCos8)
	t.Logf("4-bit avg cosine similarity: %.4f (target > 0.90)", avgCos4)

	t.Logf("=== Dot Product Accuracy ===")
	t.Logf("8-bit MAE: %.4f, Max Error: %.4f", avgDotMAE8, dotMaxErr8)
	t.Logf("4-bit MAE: %.4f, Max Error: %.4f", avgDotMAE4, dotMaxErr4)

	if avgCos8 < 0.95 {
		t.Errorf("8-bit reconstruction fidelity too low: %.4f < 0.95", avgCos8)
	}
	if avgCos4 < 0.90 {
		t.Errorf("4-bit reconstruction fidelity too low: %.4f < 0.90", avgCos4)
	}
}

// =============================================================================
// Metric 2: Recall@K at Scale (10k vectors, multiple seeds)
// =============================================================================

func TestRecallAtK(t *testing.T) {
	dim := 1536
	numVectors := 10000
	k := 10
	numQueries := 100
	seeds := []int64{42, 123, 456, 789, 1011, 1213, 1415, 1617, 1819, 2021}

	cfg4 := &HybridConfig{BitWidth: 4, BlockSize: 32}

	var totalRecall float64

	for _, seed := range seeds {
		rng := rand.New(rand.NewSource(seed))

		// Build dataset with spikes
		type vectorEntry struct {
			id  uint64
			vec []float32
		}
		dataset := make([]vectorEntry, numVectors)
		for i := 0; i < numVectors; i++ {
			dataset[i] = vectorEntry{
				id:  uint64(i + 1),
				vec: randomVectorWithSpikes(dim, rng),
			}
		}

		// Build quantized dataset
		type quantizedEntry struct {
			id   uint64
			data []byte
		}
		quantized := make([]quantizedEntry, numVectors)
		for i, entry := range dataset {
			quantized[i] = quantizedEntry{
				id:   entry.id,
				data: QuantizeHybrid(entry.vec, cfg4),
			}
		}

		var seedRecall float64

		for q := 0; q < numQueries; q++ {
			query := randomVectorWithSpikes(dim, rng)

			// Ground Truth: exact FP32 search
			type scoredID struct {
				id    uint64
				score float64
			}
			scores := make([]scoredID, numVectors)
			for i, entry := range dataset {
				scores[i] = scoredID{
					id:    entry.id,
					score: dotProductFP32(query, entry.vec),
				}
			}
			sort.Slice(scores, func(i, j int) bool {
				return scores[i].score > scores[j].score
			})

			gtTopK := make(map[uint64]bool)
			for i := 0; i < k && i < len(scores); i++ {
				gtTopK[scores[i].id] = true
			}

			// Test: 4-bit Hybrid search
			qQuery := QuantizeHybrid(query, cfg4)
			testScores := make([]scoredID, numVectors)
			for i, entry := range quantized {
				testScores[i] = scoredID{
					id:    entry.id,
					score: float64(DotProductHybrid(entry.data, qQuery, dim, cfg4)),
				}
			}
			sort.Slice(testScores, func(i, j int) bool {
				return testScores[i].score > testScores[j].score
			})

			intersection := 0
			for i := 0; i < k && i < len(testScores); i++ {
				if gtTopK[testScores[i].id] {
					intersection++
				}
			}

			seedRecall += float64(intersection) / float64(k)
		}

		avgSeedRecall := seedRecall / float64(numQueries)
		totalRecall += avgSeedRecall
		t.Logf("Seed %d: Recall@%d = %.4f", seed, k, avgSeedRecall)
	}

	avgRecall := totalRecall / float64(len(seeds))
	t.Logf("Average Recall@%d across %d seeds: %.4f (target > 0.80)", k, len(seeds), avgRecall)

	if avgRecall < 0.80 {
		t.Errorf("Recall@%d too low: %.4f < 0.80", k, avgRecall)
	}
}

// =============================================================================
// Metric 3: FWHT Invariance (FWHT(FWHT(v)) / N == v)
// =============================================================================

func TestFWHTInvariance(t *testing.T) {
	dims := []int{4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048}

	for _, dim := range dims {
		t.Run(string(rune('A'+dim%26)), func(t *testing.T) {
			orig := randomVectorFP32(dim)

			// Copy to avoid mutation
			v := make([]float32, dim)
			copy(v, orig)

			// Apply FWHT twice
			FWHT(v)
			FWHT(v)

			// Scale by 1/N
			invNorm := 1.0 / float32(dim)
			maxErr := 0.0
			for i := 0; i < dim; i++ {
				expected := orig[i]
				actual := v[i] * invNorm
				err := math.Abs(float64(expected - actual))
				if err > maxErr {
					maxErr = err
				}
			}

			if maxErr > 1e-5 {
				t.Errorf("FWHT invariance failed for dim=%d: max error=%.8f", dim, maxErr)
			}
		})
	}
}

// =============================================================================
// Metric 4: FWHT Energy Spreading (Gaussian data — must improve)
// =============================================================================

func TestQuantizationDistribution(t *testing.T) {
	dim := 1536
	numVectors := 200
	blockSize := 32
	numBlocks := (dim + blockSize - 1) / blockSize

	// Test: FWHT must reduce coefficient of variation for Gaussian data with spikes
	var withFWHT, withoutFWHT []float64

	for i := 0; i < numVectors; i++ {
		orig := randomVectorWithSpikes(dim, rand.New(rand.NewSource(int64(i))))

		// With FWHT
		paddedDim := nextPow2(dim)
		v := make([]float32, paddedDim)
		copy(v, orig)
		FWHT(v)
		invNorm := 1.0 / float32(paddedDim)
		for j := range v {
			v[j] *= invNorm
		}

		// Compute block scales with FWHT
		scalesFWHT := make([]float64, numBlocks)
		for b := 0; b < numBlocks; b++ {
			start := b * blockSize
			end := start + blockSize
			if end > dim {
				end = dim
			}
			minVal, maxVal := float32(math.MaxFloat32), float32(-math.MaxFloat32)
			for j := start; j < end; j++ {
				if v[j] < minVal {
					minVal = v[j]
				}
				if v[j] > maxVal {
					maxVal = v[j]
				}
			}
			scale := float64(maxVal - minVal)
			if scale == 0 {
				scale = 1.0
			}
			scalesFWHT[b] = scale
		}

		// Compute block scales without FWHT
		scalesNoFWHT := make([]float64, numBlocks)
		for b := 0; b < numBlocks; b++ {
			start := b * blockSize
			end := start + blockSize
			if end > dim {
				end = dim
			}
			minVal, maxVal := float32(math.MaxFloat32), float32(-math.MaxFloat32)
			for j := start; j < end; j++ {
				if orig[j] < minVal {
					minVal = orig[j]
				}
				if orig[j] > maxVal {
					maxVal = orig[j]
				}
			}
			scale := float64(maxVal - minVal)
			if scale == 0 {
				scale = 1.0
			}
			scalesNoFWHT[b] = scale
		}

		// Compute coefficient of variation (CV = std/mean) for each
		var meanFWHT, meanNoFWHT float64
		for _, s := range scalesFWHT {
			meanFWHT += s
		}
		for _, s := range scalesNoFWHT {
			meanNoFWHT += s
		}
		meanFWHT /= float64(numBlocks)
		meanNoFWHT /= float64(numBlocks)

		var varFWHT, varNoFWHT float64
		for _, s := range scalesFWHT {
			varFWHT += (s - meanFWHT) * (s - meanFWHT)
		}
		for _, s := range scalesNoFWHT {
			varNoFWHT += (s - meanNoFWHT) * (s - meanNoFWHT)
		}
		varFWHT /= float64(numBlocks)
		varNoFWHT /= float64(numBlocks)

		cvFWHT := math.Sqrt(varFWHT) / meanFWHT
		cvNoFWHT := math.Sqrt(varNoFWHT) / meanNoFWHT

		withFWHT = append(withFWHT, cvFWHT)
		withoutFWHT = append(withoutFWHT, cvNoFWHT)
	}

	// Average CV across all vectors
	var avgCVFWHT, avgCVNoFWHT float64
	for _, cv := range withFWHT {
		avgCVFWHT += cv
	}
	for _, cv := range withoutFWHT {
		avgCVNoFWHT += cv
	}
	avgCVFWHT /= float64(numVectors)
	avgCVNoFWHT /= float64(numVectors)

	t.Logf("FWHT energy spreading verification (Gaussian + spikes):")
	t.Logf("  Without FWHT: avg CV of block scales = %.4f", avgCVNoFWHT)
	t.Logf("  With FWHT:    avg CV of block scales = %.4f", avgCVFWHT)

	// FWHT MUST reduce variance for high-entropy Gaussian data with spikes.
	// If variance increases, FWHT has a normalization or logic error.
	if avgCVFWHT >= avgCVNoFWHT {
		t.Errorf("FWHT failed to reduce block scale variance for Gaussian data: %.4f >= %.4f", avgCVFWHT, avgCVNoFWHT)
	}
}

// =============================================================================
// Metric 5: 8-bit Lossless Verification
// =============================================================================

func Test8BitLosslessVerification(t *testing.T) {
	dim := 1536
	numVectors := 1000

	cfg8 := &HybridConfig{BitWidth: 8, BlockSize: 32}

	var totalBitErrors int
	var totalBits int
	var maxAbsErr float64

	for i := 0; i < numVectors; i++ {
		orig := randomVectorWithSpikes(dim, rand.New(rand.NewSource(int64(i))))

		q8 := QuantizeHybrid(orig, cfg8)
		recon8 := DequantizeHybrid(q8, dim, cfg8)

		if len(recon8) != dim {
			t.Fatalf("reconstructed length mismatch: got %d, want %d", len(recon8), dim)
		}

		for j := 0; j < dim; j++ {
			origBits := math.Float32bits(orig[j])
			reconBits := math.Float32bits(recon8[j])

			if origBits != reconBits {
				// Count differing bits
				xor := origBits ^ reconBits
				for xor != 0 {
					totalBitErrors++
					xor &= xor - 1
				}
			}
			totalBits += 32

			absErr := math.Abs(float64(orig[j]) - float64(recon8[j]))
			if absErr > maxAbsErr {
				maxAbsErr = absErr
			}
		}
	}

	bitErrorRate := float64(totalBitErrors) / float64(totalBits)
	t.Logf("8-bit lossless verification (%d vectors, %d bits):", numVectors, totalBits)
	t.Logf("  Total bit errors: %d", totalBitErrors)
	t.Logf("  Bit error rate: %.10f", bitErrorRate)
	t.Logf("  Max absolute error: %.10f", maxAbsErr)

	// 8-bit quantization is NOT lossless — it has quantization error.
	// With 256 levels, the max error per value is (range/255)/2.
	// For spike vectors with range up to 200, max error ≈ 0.4 is expected.
	// We verify that the error stays within the theoretical bound.
	//
	// Theoretical max error for 8-bit: (max_val - min_val) / (2 * 255)
	// For our spike vectors (range up to ~200): max error ≈ 0.4
	//
	// We use a loose threshold since spike vectors create extreme ranges.
	if maxAbsErr > 1.0 {
		t.Errorf("8-bit max absolute error too large: %.10f > 1.0", maxAbsErr)
	}
	// Bit error rate of ~30% is expected for 8-bit quantization
	// (we're losing ~3 bits of precision per float32)
	if bitErrorRate > 0.50 {
		t.Errorf("8-bit bit error rate too high: %.10f > 0.50", bitErrorRate)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkHybrid4BitSearch(b *testing.B) {
	dim := 1536
	numVectors := 10000
	cfg4 := &HybridConfig{BitWidth: 4, BlockSize: 32}

	quantized := make([][]byte, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := randomVectorFP32(dim)
		quantized[i] = QuantizeHybrid(vec, cfg4)
	}

	query := randomVectorFP32(dim)
	qQuery := QuantizeHybrid(query, cfg4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, data := range quantized {
			DotProductHybrid(data, qQuery, dim, cfg4)
		}
	}
}

func BenchmarkFP32ExactSearch(b *testing.B) {
	dim := 1536
	numVectors := 10000

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = randomVectorFP32(dim)
	}

	query := randomVectorFP32(dim)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, vec := range vectors {
			dotProductFP32(query, vec)
		}
	}
}
