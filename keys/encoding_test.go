package keys

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeSPOKey(t *testing.T) {
	tests := []struct {
		name      string
		subject   uint64
		predicate uint64
		object    uint64
	}{
		{"Basic triple", 123, 456, 789},
		{"Zero values", 0, 0, 0},
		{"Large values", 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFE, 0xFFFFFFFFFFFFFFFD},
		{"Sequential", 1, 2, 3},
		{"Same values", 100, 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			key := EncodeSPOKey(tt.subject, tt.predicate, tt.object)

			// Verify prefix
			if key[0] != SPOPrefix {
				t.Errorf("Expected prefix %d, got %d", SPOPrefix, key[0])
			}

			// Verify length
			if len(key) != 25 {
				t.Errorf("Expected key length 25, got %d", len(key))
			}

			// Decode
			subject, predicate, object := DecodeSPOKey(key)

			// Verify round-trip
			if subject != tt.subject {
				t.Errorf("Subject: expected %d, got %d", tt.subject, subject)
			}
			if predicate != tt.predicate {
				t.Errorf("Predicate: expected %d, got %d", tt.predicate, predicate)
			}
			if object != tt.object {
				t.Errorf("Object: expected %d, got %d", tt.object, object)
			}
		})
	}
}

func TestEncodeDecodeOPSKey(t *testing.T) {
	tests := []struct {
		name      string
		subject   uint64
		predicate uint64
		object    uint64
	}{
		{"Basic triple", 123, 456, 789},
		{"Zero values", 0, 0, 0},
		{"Large values", 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFE, 0xFFFFFFFFFFFFFFFD},
		{"Sequential", 1, 2, 3},
		{"Same values", 100, 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			key := EncodeOPSKey(tt.subject, tt.predicate, tt.object)

			// Verify prefix
			if key[0] != OPSPrefix {
				t.Errorf("Expected prefix %d, got %d", OPSPrefix, key[0])
			}

			// Verify length
			if len(key) != 25 {
				t.Errorf("Expected key length 25, got %d", len(key))
			}

			// Decode
			subject, predicate, object := DecodeOPSKey(key)

			// Verify round-trip
			if subject != tt.subject {
				t.Errorf("Subject: expected %d, got %d", tt.subject, subject)
			}
			if predicate != tt.predicate {
				t.Errorf("Predicate: expected %d, got %d", tt.predicate, predicate)
			}
			if object != tt.object {
				t.Errorf("Object: expected %d, got %d", tt.object, object)
			}
		})
	}
}

func TestPrefixOrdering(t *testing.T) {
	// Test that SPO keys sort correctly by prefix
	key1 := EncodeSPOKey(1, 0, 0)
	key2 := EncodeSPOKey(2, 0, 0)
	key3 := EncodeSPOKey(1, 1, 0)

	// key1 (subject=1) should come before key2 (subject=2)
	if bytes.Compare(key1, key2) >= 0 {
		t.Errorf("Subject 1 should come before subject 2")
	}

	// key1 (pred=0) should come before key3 (pred=1)
	if bytes.Compare(key1, key3) >= 0 {
		t.Errorf("Predicate 0 should come before predicate 1")
	}

	// Same subject, different predicates should still be ordered
	key4 := EncodeSPOKey(100, 1, 0)
	key5 := EncodeSPOKey(100, 2, 0)
	if bytes.Compare(key4, key5) >= 0 {
		t.Errorf("Predicate 1 should come before predicate 2")
	}
}

func TestEncodeSPOPrefix(t *testing.T) {
	tests := []struct {
		name      string
		subject   uint64
		predicate uint64
		expected  int
	}{
		{"No bounds", 0, 0, 1},           // Just prefix byte
		{"Subject only", 123, 0, 9},     // prefix + subject
		{"Subject+Predicate", 123, 456, 17}, // prefix + subject + predicate
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := EncodeSPOPrefix(tt.subject, tt.predicate)

			if len(prefix) != tt.expected {
				t.Errorf("Expected prefix length %d, got %d", tt.expected, len(prefix))
			}

			// Verify prefix byte
			if prefix[0] != SPOPrefix {
				t.Errorf("Expected SPO prefix byte, got %d", prefix[0])
			}
		})
	}
}

func TestEncodeOPSPrefix(t *testing.T) {
	tests := []struct {
		name      string
		object    uint64
		predicate uint64
		expected  int
	}{
		{"No bounds", 0, 0, 1},           // Just prefix byte
		{"Object only", 789, 0, 9},      // prefix + object
		{"Object+Predicate", 789, 456, 17}, // prefix + object + predicate
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := EncodeOPSPrefix(tt.object, tt.predicate)

			if len(prefix) != tt.expected {
				t.Errorf("Expected prefix length %d, got %d", tt.expected, len(prefix))
			}

			// Verify prefix byte
			if prefix[0] != OPSPrefix {
				t.Errorf("Expected OPS prefix byte, got %d", prefix[0])
			}
		})
	}
}

func TestDecodeInvalidKey(t *testing.T) {
	// Test decoding invalid keys
	invalidKeys := [][]byte{
		nil,                // nil key
		{},                 // empty key
		{0x00},             // wrong prefix, too short
		{SPOPrefix},        // correct prefix, too short
		make([]byte, 5),    // too short
		make([]byte, 24),   // one byte short
		{OPSPrefix},        // correct prefix, too short
	}

	for _, key := range invalidKeys {
		subject, predicate, object := DecodeSPOKey(key)
		if subject != 0 || predicate != 0 || object != 0 {
			t.Errorf("Expected zeros for invalid key, got %d, %d, %d", subject, predicate, object)
		}

		subject, predicate, object = DecodeOPSKey(key)
		if subject != 0 || predicate != 0 || object != 0 {
			t.Errorf("Expected zeros for invalid key, got %d, %d, %d", subject, predicate, object)
		}
	}
}

func TestWrongPrefixDecoding(t *testing.T) {
	// Create an SPO key but try to decode it as OPS, and vice versa
	spoKey := EncodeSPOKey(123, 456, 789)
	opsKey := EncodeOPSKey(123, 456, 789)

	// Try to decode SPO key as OPS - should return zeros
	subject, predicate, object := DecodeOPSKey(spoKey)
	if subject != 0 || predicate != 0 || object != 0 {
		t.Errorf("Expected zeros when decoding SPO key as OPS, got %d, %d, %d", subject, predicate, object)
	}

	// Try to decode OPS key as SPO - should return zeros
	subject, predicate, object = DecodeSPOKey(opsKey)
	if subject != 0 || predicate != 0 || object != 0 {
		t.Errorf("Expected zeros when decoding OPS key as SPO, got %d, %d, %d", subject, predicate, object)
	}
}

// Benchmark tests for performance

func BenchmarkEncodeSPOKey(b *testing.B) {
	subject, predicate, object := uint64(12345), uint64(67890), uint64(12345678)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeSPOKey(subject, predicate, object)
	}
}

func BenchmarkDecodeSPOKey(b *testing.B) {
	key := EncodeSPOKey(12345, 67890, 12345678)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeSPOKey(key)
	}
}

func BenchmarkEncodeSPOPrefix(b *testing.B) {
	subject, predicate := uint64(12345), uint64(67890)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeSPOPrefix(subject, predicate)
	}
}
