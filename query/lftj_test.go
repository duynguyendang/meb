package query

import (
	"bytes"
	"testing"
)

func TestTrieIteratorBasics(t *testing.T) {
	// Test TrieIterator initialization
	columnOrder := []int{0, 1, 2, 3}
	iter := &TrieIterator{
		prefix:      []byte{0x20},
		depth:       0,
		columnOrder: columnOrder,
		keySize:     33,
		exhausted:   false,
	}

	if iter.depth != 0 {
		t.Errorf("Expected depth 0, got %d", iter.depth)
	}

	if !bytes.Equal(iter.prefix, []byte{0x20}) {
		t.Errorf("Expected prefix [0x20], got %v", iter.prefix)
	}
}

func TestExtractUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		offset   int
		expected uint64
	}{
		{
			name:     "simple value",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			offset:   0,
			expected: 1,
		},
		{
			name:     "max value",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			offset:   0,
			expected: 0xFFFFFFFFFFFFFFFF,
		},
		{
			name:     "with offset",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			offset:   8,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractUint64(tt.input, tt.offset)
			if result != tt.expected {
				t.Errorf("extractUint64() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestDetermineOptimalOrder(t *testing.T) {
	tests := []struct {
		name           string
		boundPositions map[int]uint64
		cardinalities  map[int]int
		expectedFirst  int // Expected first position in optimal order
	}{
		{
			name:           "bound positions first",
			boundPositions: map[int]uint64{0: 123},
			cardinalities:  map[int]int{1: 1000, 2: 500},
			expectedFirst:  0,
		},
		{
			name:           "lower cardinality first",
			boundPositions: map[int]uint64{},
			cardinalities:  map[int]int{0: 1000, 1: 100, 2: 500},
			expectedFirst:  1, // Position 1 has lowest cardinality (100)
		},
		{
			name:           "bound takes precedence",
			boundPositions: map[int]uint64{2: 456},
			cardinalities:  map[int]int{0: 10, 1: 20},
			expectedFirst:  2, // Bound position comes first despite higher cardinality
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order := DetermineOptimalOrder(tt.boundPositions, tt.cardinalities)

			if len(order.Attributes) == 0 {
				t.Fatal("Expected non-empty attribute order")
			}

			if order.Attributes[0].Position != tt.expectedFirst {
				t.Errorf("Expected first position %d, got %d", tt.expectedFirst, order.Attributes[0].Position)
			}
		})
	}
}

func TestLFTJEngineGetColumnOrder(t *testing.T) {
	engine := &LFTJEngine{}

	tests := []struct {
		prefix   byte
		expected []int
	}{
		{0x20, []int{0, 1, 2, 3}}, // SPOG
		{0x21, []int{2, 1, 0, 3}}, // OPSG
		{0x22, []int{3, 0, 1, 2}}, // GSPO
		{0xFF, []int{0, 1, 2, 3}}, // Unknown - default
	}

	for _, tt := range tests {
		t.Run(string(tt.prefix), func(t *testing.T) {
			result := engine.getColumnOrder(tt.prefix)
			if len(result) != len(tt.expected) {
				t.Fatalf("Expected length %d, got %d", len(tt.expected), len(result))
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("Position %d: expected %d, got %d", i, tt.expected[i], v)
				}
			}
		})
	}
}

func TestLFTJEngineBuildPrefix(t *testing.T) {
	engine := &LFTJEngine{}

	tests := []struct {
		name     string
		pattern  RelationPattern
		expected []byte
	}{
		{
			name: "empty bound positions",
			pattern: RelationPattern{
				Prefix:         0x20,
				BoundPositions: map[int]uint64{},
			},
			expected: []byte{0x20},
		},
		{
			name: "subject bound",
			pattern: RelationPattern{
				Prefix:         0x20,
				BoundPositions: map[int]uint64{0: 123},
			},
			expected: []byte{0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 123},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.buildPrefix(tt.pattern)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("buildPrefix() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestLeapfrogJoinEmpty(t *testing.T) {
	// Test with empty iterators
	iterators := []*TrieIterator{}

	count := 0
	for _, err := range LeapfrogJoin(iterators) {
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 results, got %d", count)
	}
}

func TestVariableOrder(t *testing.T) {
	// Test VariableOrder struct
	order := VariableOrder{
		Attributes: []Attribute{
			{Position: 0, Selectivity: 0.1},
			{Position: 1, Selectivity: 0.5},
			{Position: 2, Selectivity: 0.9},
		},
		Cost: 1.5,
	}

	if len(order.Attributes) != 3 {
		t.Errorf("Expected 3 attributes, got %d", len(order.Attributes))
	}

	if order.Cost != 1.5 {
		t.Errorf("Expected cost 1.5, got %f", order.Cost)
	}
}

func TestLFTJQueryStructure(t *testing.T) {
	// Test LFTJQuery struct creation
	query := LFTJQuery{
		Relations: []RelationPattern{
			{
				Prefix:            0x20,
				BoundPositions:    map[int]uint64{0: 1},
				VariablePositions: map[int]string{1: "p", 2: "o"},
			},
		},
		BoundVars:  map[string]uint64{"s": 1},
		ResultVars: []string{"s", "p", "o"},
	}

	if len(query.Relations) != 1 {
		t.Errorf("Expected 1 relation, got %d", len(query.Relations))
	}

	if len(query.ResultVars) != 3 {
		t.Errorf("Expected 3 result vars, got %d", len(query.ResultVars))
	}
}

func TestLFTJStats(t *testing.T) {
	stats := LFTJStats{
		NumJoins:      10,
		NumResults:    100,
		NumSeeks:      50,
		NumNexts:      200,
		ExecutionTime: 1000000, // 1ms in nanoseconds
	}

	if stats.NumJoins != 10 {
		t.Errorf("Expected 10 joins, got %d", stats.NumJoins)
	}

	if stats.NumResults != 100 {
		t.Errorf("Expected 100 results, got %d", stats.NumResults)
	}
}
