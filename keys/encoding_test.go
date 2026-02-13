package keys

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeQuadKey(t *testing.T) {
	tests := []struct {
		name       string
		prefix     byte
		s, p, o, g uint64
	}{
		{
			name:   "SPOG format",
			prefix: QuadSPOGPrefix,
			s:      1, p: 2, o: 3, g: 4,
		},
		{
			name:   "POSG format",
			prefix: QuadPOSGPrefix,
			s:      1, p: 2, o: 3, g: 4,
		},
		{
			name:   "GSPO format",
			prefix: QuadGSPOPrefix,
			s:      1, p: 2, o: 3, g: 4,
		},
		{
			name:   "Large IDs",
			prefix: QuadSPOGPrefix,
			s:      0xFFFFFFFFFFFFFFFF,
			p:      0xFFFFFFFFFFFFFFFE,
			o:      0xFFFFFFFFFFFFFFFD,
			g:      0xFFFFFFFFFFFFFFFC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			key := EncodeQuadKey(tt.prefix, tt.s, tt.p, tt.o, tt.g)

			// Verify key size
			if len(key) != QuadKeySize {
				t.Errorf("EncodeQuadKey() key size = %v, want %v", len(key), QuadKeySize)
			}

			// Verify prefix
			if key[0] != tt.prefix {
				t.Errorf("EncodeQuadKey() prefix = %v, want %v", key[0], tt.prefix)
			}

			// Decode
			s, p, o, g := DecodeQuadKey(key)

			// Verify decoded values
			if s != tt.s || p != tt.p || o != tt.o || g != tt.g {
				t.Errorf("DecodeQuadKey() = (%v, %v, %v, %v), want (%v, %v, %v, %v)",
					s, p, o, g, tt.s, tt.p, tt.o, tt.g)
			}
		})
	}
}

func TestEncodeQuadSPOGPrefix(t *testing.T) {
	tests := []struct {
		name        string
		s, p, o, g  uint64
		expectedLen int
	}{
		{"Empty", 0, 0, 0, 0, 1},                     // Just prefix
		{"Subject only", 1, 0, 0, 0, 9},              // Prefix + S
		{"Subject+Predicate", 1, 2, 0, 0, 17},        // Prefix + S + P
		{"Subject+Predicate+Object", 1, 2, 3, 0, 25}, // Prefix + S + P + O
		{"Full", 1, 2, 3, 4, 33},                     // Prefix + S + P + O + G
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := EncodeQuadSPOGPrefix(tt.s, tt.p, tt.o, tt.g)

			if len(prefix) != tt.expectedLen {
				t.Errorf("EncodeQuadSPOGPrefix() length = %v, want %v", len(prefix), tt.expectedLen)
			}

			// Verify prefix byte
			if prefix[0] != QuadSPOGPrefix {
				t.Errorf("EncodeQuadSPOGPrefix() prefix byte = %v, want %v", prefix[0], QuadSPOGPrefix)
			}
		})
	}
}

func TestEncodeQuadGSPOPrefix(t *testing.T) {
	tests := []struct {
		name        string
		g           uint64
		expectedLen int
	}{
		{"Empty", 0, 1},      // Just prefix
		{"Graph only", 1, 9}, // Prefix + G
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := EncodeQuadGSPOPrefix(tt.g)

			if len(prefix) != tt.expectedLen {
				t.Errorf("EncodeQuadGSPOPrefix() length = %v, want %v", len(prefix), tt.expectedLen)
			}

			// Verify prefix byte
			if prefix[0] != QuadGSPOPrefix {
				t.Errorf("EncodeQuadGSPOPrefix() prefix byte = %v, want %v", prefix[0], QuadGSPOPrefix)
			}
		})
	}
}

func TestKeySizeConstants(t *testing.T) {
	// Verify quad key size
	if QuadKeySize != 33 {
		t.Errorf("QuadKeySize = %v, want 33", QuadKeySize)
	}

	// Verify prefix size
	if PrefixSize != 1 {
		t.Errorf("PrefixSize = %v, want 1", PrefixSize)
	}

	// Verify ID size
	if IDSize != 8 {
		t.Errorf("IDSize = %v, want 8", IDSize)
	}
}

func TestKeyOrdering(t *testing.T) {
	// Test that keys are ordered correctly for range scans
	key1 := EncodeQuadKey(QuadSPOGPrefix, 1, 1, 1, 1)
	key2 := EncodeQuadKey(QuadSPOGPrefix, 1, 1, 1, 2)
	key3 := EncodeQuadKey(QuadSPOGPrefix, 1, 1, 2, 1)
	key4 := EncodeQuadKey(QuadSPOGPrefix, 1, 2, 1, 1)
	key5 := EncodeQuadKey(QuadSPOGPrefix, 2, 1, 1, 1)

	// Verify ordering
	if bytes.Compare(key1, key2) >= 0 {
		t.Error("key1 should be less than key2")
	}
	if bytes.Compare(key2, key3) >= 0 {
		t.Error("key2 should be less than key3")
	}
	if bytes.Compare(key3, key4) >= 0 {
		t.Error("key3 should be less than key4")
	}
	if bytes.Compare(key4, key5) >= 0 {
		t.Error("key4 should be less than key5")
	}
}

func TestEncodeChunkKey(t *testing.T) {
	id := uint64(12345)
	key := EncodeChunkKey(id)

	if len(key) != ChunkKeySize {
		t.Errorf("EncodeChunkKey() length = %v, want %v", len(key), ChunkKeySize)
	}

	if key[0] != ChunkPrefix {
		t.Errorf("EncodeChunkKey() prefix = %v, want %v", key[0], ChunkPrefix)
	}
}
