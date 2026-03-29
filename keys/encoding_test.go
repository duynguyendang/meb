package keys

import (
	"testing"
)

func TestPackID(t *testing.T) {
	tests := []struct {
		name    string
		topicID uint32
		localID uint64
		want    uint64
	}{
		{"topic 1 local 1", 1, 1, (1 << 40) | 1},
		{"topic 100 local 500", 100, 500, (100 << 40) | 500},
		{"max topic", 0xFFFFFF, 1, (0xFFFFFF << 40) | 1},
		{"max local", 1, 0xFFFFFFFFFF, (1 << 40) | 0xFFFFFFFFFF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PackID(tt.topicID, tt.localID)
			if got != tt.want {
				t.Errorf("PackID(%d, %d) = %d, want %d", tt.topicID, tt.localID, got, tt.want)
			}
		})
	}
}

func TestUnpackTopicID(t *testing.T) {
	tests := []struct {
		name string
		id   uint64
		want uint32
	}{
		{"topic 1", (1 << 40) | 42, 1},
		{"topic 100", (100 << 40) | 42, 100},
		{"max topic", (0xFFFFFF << 40) | 1, 0xFFFFFF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UnpackTopicID(tt.id)
			if got != tt.want {
				t.Errorf("UnpackTopicID(%d) = %d, want %d", tt.id, got, tt.want)
			}
		})
	}
}

func TestUnpackLocalID(t *testing.T) {
	tests := []struct {
		name string
		id   uint64
		want uint64
	}{
		{"local 1", (1 << 40) | 1, 1},
		{"local 42", (100 << 40) | 42, 42},
		{"max local", (1 << 40) | 0xFFFFFFFFFF, 0xFFFFFFFFFF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UnpackLocalID(tt.id)
			if got != tt.want {
				t.Errorf("UnpackLocalID(%d) = %d, want %d", tt.id, got, tt.want)
			}
		})
	}
}

func TestPackUnpackRoundtrip(t *testing.T) {
	tests := []struct {
		topicID uint32
		localID uint64
	}{
		{1, 1},
		{100, 500},
		{0xFFFFFF, 0xFFFFFFFFFF},
	}
	for _, tt := range tests {
		packed := PackID(tt.topicID, tt.localID)
		gotTopic := UnpackTopicID(packed)
		gotLocal := UnpackLocalID(packed)
		if gotTopic != tt.topicID {
			t.Errorf("roundtrip topic: got %d, want %d", gotTopic, tt.topicID)
		}
		if gotLocal != tt.localID {
			t.Errorf("roundtrip local: got %d, want %d", gotLocal, tt.localID)
		}
	}
}

func TestEncodeDecodeTripleKey(t *testing.T) {
	tests := []struct {
		name    string
		prefix  byte
		s, p, o uint64
	}{
		{"SPO basic", TripleSPOPrefix, 100, 200, 300},
		{"OPS basic", TripleOPSPrefix, 100, 200, 300},
		{"SPO max values", TripleSPOPrefix, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := EncodeTripleKey(tt.prefix, tt.s, tt.p, tt.o)
			if len(key) != TripleKeySize {
				t.Fatalf("key size = %d, want %d", len(key), TripleKeySize)
			}
			if key[0] != tt.prefix {
				t.Errorf("prefix = 0x%02x, want 0x%02x", key[0], tt.prefix)
			}
			gotS, gotP, gotO := DecodeTripleKey(key)
			if tt.prefix == TripleSPOPrefix {
				if gotS != tt.s || gotP != tt.p || gotO != tt.o {
					t.Errorf("SPO decode: got (%d,%d,%d), want (%d,%d,%d)", gotS, gotP, gotO, tt.s, tt.p, tt.o)
				}
			} else {
				if gotS != tt.s || gotP != tt.p || gotO != tt.o {
					t.Errorf("OPS decode: got (%d,%d,%d), want (%d,%d,%d)", gotS, gotP, gotO, tt.s, tt.p, tt.o)
				}
			}
		})
	}
}

func TestDecodeTripleKeyTooShort(t *testing.T) {
	s, p, o := DecodeTripleKey([]byte{0x20, 0x00})
	if s != 0 || p != 0 || o != 0 {
		t.Errorf("expected zeros for short key, got (%d,%d,%d)", s, p, o)
	}
}

func TestDecodeTripleKeyUnknownPrefix(t *testing.T) {
	key := make([]byte, TripleKeySize)
	key[0] = 0xFF
	s, p, o := DecodeTripleKey(key)
	if s != 0 || p != 0 || o != 0 {
		t.Errorf("expected zeros for unknown prefix, got (%d,%d,%d)", s, p, o)
	}
}

func TestEncodeSemanticHints(t *testing.T) {
	hints := EncodeSemanticHints(EntityFunc, 0x42, FlagIsPublic)
	entityType, semanticHash, flags := DecodeSemanticHints(hints)
	if entityType != EntityFunc {
		t.Errorf("entityType = %d, want %d", entityType, EntityFunc)
	}
	if semanticHash != 0x42 {
		t.Errorf("semanticHash = 0x%x, want 0x42", semanticHash)
	}
	if flags != FlagIsPublic {
		t.Errorf("flags = %d, want %d", flags, FlagIsPublic)
	}
}

func TestHashSemanticName(t *testing.T) {
	h1 := HashSemanticName("alice")
	h2 := HashSemanticName("alice")
	h3 := HashSemanticName("bob")
	if h1 != h2 {
		t.Error("same name should produce same hash")
	}
	if h1 == h3 {
		t.Error("different names should (probably) produce different hashes")
	}
}

func TestEncodeChunkKey(t *testing.T) {
	key := EncodeChunkKey(42)
	if len(key) != ChunkKeySize {
		t.Fatalf("chunk key size = %d, want %d", len(key), ChunkKeySize)
	}
	if key[0] != ChunkPrefix {
		t.Errorf("prefix = 0x%02x, want 0x%02x", key[0], ChunkPrefix)
	}
}

func TestEncodeVectorFullKey(t *testing.T) {
	key := EncodeVectorFullKey(42)
	if len(key) != ChunkKeySize {
		t.Fatalf("vector key size = %d, want %d", len(key), ChunkKeySize)
	}
	if key[0] != VectorFullPrefix {
		t.Errorf("prefix = 0x%02x, want 0x%02x", key[0], VectorFullPrefix)
	}
}

func TestEncodeSPOByTopic(t *testing.T) {
	key := EncodeSPOByTopic(100)
	if len(key) != PrefixSize+IDSize {
		t.Fatalf("topic prefix size = %d, want %d", len(key), PrefixSize+IDSize)
	}
	if key[0] != TripleSPOPrefix {
		t.Errorf("prefix = 0x%02x, want 0x%02x", key[0], TripleSPOPrefix)
	}
}

func TestInlineBool(t *testing.T) {
	idTrue := PackInlineBool(true)
	idFalse := PackInlineBool(false)

	if !IsInline(idTrue) {
		t.Error("packed true should be inline")
	}
	if !IsInline(idFalse) {
		t.Error("packed false should be inline")
	}
	if !UnpackInlineBool(idTrue) {
		t.Error("unpacked true should be true")
	}
	if UnpackInlineBool(idFalse) {
		t.Error("unpacked false should be false")
	}
}

func TestInlineInt32(t *testing.T) {
	tests := []int32{0, 1, -1, 42, -42, 2147483647, -2147483648}
	for _, v := range tests {
		id := PackInlineInt32(v)
		if !IsInline(id) {
			t.Errorf("packed %d should be inline", v)
		}
		got := UnpackInlineInt32(id)
		if got != v {
			t.Errorf("int32 roundtrip: got %d, want %d", got, v)
		}
	}
}

func TestInlineFloat32(t *testing.T) {
	tests := []float32{0.0, 1.0, -1.0, 3.14, -0.001, 1e10}
	for _, v := range tests {
		id := PackInlineFloat32(v)
		if !IsInline(id) {
			t.Errorf("packed %f should be inline", v)
		}
		got := UnpackInlineFloat32(id)
		if got != v {
			t.Errorf("float32 roundtrip: got %f, want %f", got, v)
		}
	}
}

func TestInlineNoCollisionWithTopicID(t *testing.T) {
	// Normal packed IDs should never be inline
	for topicID := uint32(1); topicID <= 100; topicID++ {
		for localID := uint64(1); localID <= 1000; localID += 100 {
			packed := PackID(topicID, localID)
			if IsInline(packed) {
				t.Errorf("packed ID (topic=%d, local=%d) should NOT be inline", topicID, localID)
			}
		}
	}
}

func TestInlineNoCollisionWithLocalID(t *testing.T) {
	// Dictionary local IDs should never be inline (they're sequential from 1)
	for id := uint64(1); id <= 100000; id += 1000 {
		if IsInline(id) {
			t.Errorf("local ID %d should NOT be inline", id)
		}
	}
}
