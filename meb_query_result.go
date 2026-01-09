package meb

import "fmt"

// Result represents a single neuro-symbolic search result.
type Result struct {
	ID      uint64  // Internal dictionary ID
	Key     string  // Human-readable key (decoded from dictionary)
	Score   float32 // Similarity score (0-1, higher is better)
	Content string  // Document content (empty if not found)
}

// String returns a string representation of the result.
func (r Result) String() string {
	return fmt.Sprintf("Result{ID: %d, Key: %s, Score: %.4f}", r.ID, r.Key, r.Score)
}

// Results is a slice of Result with helper methods.
type Results []Result

// IDs returns all result IDs.
func (rs Results) IDs() []uint64 {
	ids := make([]uint64, len(rs))
	for i, r := range rs {
		ids[i] = r.ID
	}
	return ids
}

// Keys returns all result keys.
func (rs Results) Keys() []string {
	keys := make([]string, len(rs))
	for i, r := range rs {
		keys[i] = r.Key
	}
	return keys
}

// Scores returns all result scores.
func (rs Results) Scores() []float32 {
	scores := make([]float32, len(rs))
	for i, r := range rs {
		scores[i] = r.Score
	}
	return scores
}
