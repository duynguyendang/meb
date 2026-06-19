package keys

import (
	"encoding/binary"
	"math"
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

// --- H2: Fuzz tests for triple key encoding round-trip ---

// FuzzEncodeDecodeTripleKey verifies that EncodeTripleKey + DecodeTripleKey
// round-trip for all valid prefixes and arbitrary S/P/O values.
func FuzzEncodeDecodeTripleKey(f *testing.F) {
	// Seed corpus: all valid prefixes × edge values
	seeds := []struct {
		prefix byte
		s, p, o uint64
	}{
		{TripleSPOPrefix, 0, 0, 0},
		{TripleOPSPrefix, 0, 0, 0},
		{TripleSPOPrefix, 1, 1, 1},
		{TripleOPSPrefix, 1, 1, 1},
		{TripleSPOPrefix, ^uint64(0), ^uint64(0), ^uint64(0)},
		{TripleOPSPrefix, ^uint64(0), ^uint64(0), ^uint64(0)},
		{TripleSPOPrefix, 0xFFFFFFFFFFFFFFFF, 0, 0},
		{TripleOPSPrefix, 0, 0xFFFFFFFFFFFFFFFF, 0},
		{TripleSPOPrefix, 123456789, 987654321, 555555555},
		{TripleOPSPrefix, 555555555, 987654321, 123456789},
	}
	for _, s := range seeds {
		f.Add(s.prefix, s.s, s.p, s.o)
	}

	f.Fuzz(func(t *testing.T, prefix byte, s, p, o uint64) {
		// Only test valid prefixes
		if prefix != TripleSPOPrefix && prefix != TripleOPSPrefix {
			// Unknown prefix — decoder returns 0s. Assert that.
			key := EncodeTripleKey(prefix, s, p, o)
			if len(key) != TripleKeySize {
				t.Fatalf("key size = %d, want %d", len(key), TripleKeySize)
			}
			gotS, gotP, gotO := DecodeTripleKey(key)
			if gotS != 0 || gotP != 0 || gotO != 0 {
				t.Errorf("unknown prefix 0x%02x: decode returned (%d,%d,%d), want (0,0,0)", prefix, gotS, gotP, gotO)
			}
			return
		}

		key := EncodeTripleKey(prefix, s, p, o)
		if len(key) != TripleKeySize {
			t.Fatalf("key size = %d, want %d", len(key), TripleKeySize)
		}
		if key[0] != prefix {
			t.Errorf("prefix = 0x%02x, want 0x%02x", key[0], prefix)
		}

		gotS, gotP, gotO := DecodeTripleKey(key)
		if gotS != s || gotP != p || gotO != o {
			t.Errorf("roundtrip for prefix 0x%02x: got (%d,%d,%d), want (%d,%d,%d)",
				prefix, gotS, gotP, gotO, s, p, o)
		}
	})
}

// FuzzEncodeTripleKeyInto verifies that EncodeTripleKeyInto (pooled buffer)
// round-trips identically to EncodeTripleKey.
func FuzzEncodeTripleKeyInto(f *testing.F) {
	seeds := []struct {
		prefix byte
		s, p, o uint64
	}{
		{TripleSPOPrefix, 0, 0, 0},
		{TripleOPSPrefix, 0, 0, 0},
		{TripleSPOPrefix, 1, 2, 3},
		{TripleOPSPrefix, ^uint64(0), ^uint64(0), ^uint64(0)},
	}
	for _, s := range seeds {
		f.Add(s.prefix, s.s, s.p, s.o)
	}

	f.Fuzz(func(t *testing.T, prefix byte, s, p, o uint64) {
		if prefix != TripleSPOPrefix && prefix != TripleOPSPrefix {
			t.Skip("skip unknown prefix for Into test")
		}

		// Compare EncodeTripleKey vs EncodeTripleKeyInto
		expected := EncodeTripleKey(prefix, s, p, o)

		buf := make([]byte, TripleKeySize)
		result := EncodeTripleKeyInto(buf, prefix, s, p, o)
		if len(result) != TripleKeySize {
			t.Fatalf("Into key size = %d, want %d", len(result), TripleKeySize)
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("byte %d: Into=0x%02x, Encode=0x%02x", i, result[i], expected[i])
			}
		}

		// Decode the Into result
		gotS, gotP, gotO := DecodeTripleKey(result)
		if gotS != s || gotP != p || gotO != o {
			t.Errorf("Into roundtrip: got (%d,%d,%d), want (%d,%d,%d)", gotS, gotP, gotO, s, p, o)
		}

		// Verify buffer length is exactly TripleKeySize
		if len(result) != TripleKeySize {
			t.Errorf("buffer length = %d, want %d", len(result), TripleKeySize)
		}
	})
}

// --- H3: Boundary value tests for inline ID encoding ---

func TestPackInlineFloat32Boundaries(t *testing.T) {
	tests := []struct {
		name string
		val  float32
	}{
		{"NaN", float32(math.NaN())},
		{"+Inf", float32(math.Inf(1))},
		{"-Inf", float32(math.Inf(-1))},
		{"+0", 0.0},
		{"-0", float32(math.Copysign(0, -1))},
		{"MaxFloat32", math.MaxFloat32},
		{"SmallestNonzeroFloat32", math.SmallestNonzeroFloat32},
		{"denormal_1", float32(1.0e-40)},  // denormalized
		{"denormal_2", float32(2.5e-40)},  // denormalized
		{"pi", float32(math.Pi)},
		{"e", float32(math.E)},
		{"1e-30", float32(1e-30)},
		{"1e30", float32(1e30)},
		{"neg_pi", float32(-math.Pi)},
		{"neg_1e30", float32(-1e30)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := PackInlineFloat32(tt.val)
			if !IsInline(id) {
				t.Errorf("packed %v (%f) should be inline", tt.name, tt.val)
			}
			got := UnpackInlineFloat32(id)
			// Bit-level comparison for NaN: NaN != NaN, so compare bits
			if math.IsNaN(float64(tt.val)) {
				if !math.IsNaN(float64(got)) {
					t.Errorf("expected NaN, got %v", got)
				}
				return
			}
			// For -0, compare bits
			if tt.val == 0 && math.Signbit(float64(tt.val)) {
				if !(got == 0 && math.Signbit(float64(got))) {
					t.Errorf("-0 roundtrip: got %v (%b), want -0", got, math.Float32bits(got))
				}
				return
			}
			if got != tt.val {
				t.Errorf("roundtrip: got %v, want %v", got, tt.val)
			}
			// Assert bit-identical roundtrip
			bits := math.Float32bits(tt.val)
			gotBits := math.Float32bits(got)
			if bits != gotBits {
				t.Errorf("bit mismatch: input 0x%08x, output 0x%08x", bits, gotBits)
			}
		})
	}
}

func TestPackInlineInt32Boundaries(t *testing.T) {
	tests := []struct {
		name string
		val  int32
	}{
		{"MaxInt32", 2147483647},
		{"MinInt32", -2147483648},
		{"zero", 0},
		{"neg_one", -1},
		{"one", 1},
		{"neg_2147483647", -2147483647},
		{"2147483647", 2147483647},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := PackInlineInt32(tt.val)
			if !IsInline(id) {
				t.Errorf("packed %d should be inline", tt.val)
			}
			got := UnpackInlineInt32(id)
			if got != tt.val {
				t.Errorf("roundtrip: got %d, want %d", got, tt.val)
			}
		})
	}
}

func TestPackInlineBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		id := PackInlineBool(v)
		if !IsInline(id) {
			t.Errorf("packed %v should be inline", v)
		}
		got := UnpackInlineBool(id)
		if got != v {
			t.Errorf("roundtrip: got %v, want %v", got, v)
		}
	}
}

func TestInlineNoCollisionWithTopicIDBoundary(t *testing.T) {
	// Adversarial inputs: inline IDs at boundary (1<<39) and topic IDs with same bit pattern
	adversarialIDs := []uint64{
		1 << 39,           // InlineBit value
		1 << 38,           // InlineIsNum
		1 << 37,           // InlineNumF32
		(1 << 39) | 1,     // inline true
		(1 << 39) | (1 << 38), // inline number (int32 zero)
	}
	for _, id := range adversarialIDs {
		if IsInline(id) != ((id & InlineBit) != 0) {
			t.Errorf("IsInline(%d) inconsistency", id)
		}
	}

	// Normal packed IDs should never be inline
	for topicID := uint32(1); topicID <= 100; topicID++ {
		for localID := uint64(1); localID <= 100000; localID += 5000 {
			packed := PackID(topicID, localID)
			if IsInline(packed) {
				t.Errorf("packed ID (topic=%d, local=%d) should NOT be inline", topicID, localID)
			}
		}
	}
}

func TestEncodeSPOByTopicRoundtrip(t *testing.T) {
	topicIDs := []uint32{0, 1, 100, 0xFFFFFF, 0xABCDEF}
	for _, topicID := range topicIDs {
		spKey := EncodeSPOByTopic(topicID)
		opsKey := EncodeOPSByTopic(topicID)
		if len(spKey) != PrefixSize+IDSize {
			t.Fatalf("SPO topic key size = %d, want %d", len(spKey), PrefixSize+IDSize)
		}
		if len(opsKey) != PrefixSize+IDSize {
			t.Fatalf("OPS topic key size = %d, want %d", len(opsKey), PrefixSize+IDSize)
		}
		if spKey[0] != TripleSPOPrefix {
			t.Errorf("SPO prefix = 0x%02x, want 0x%02x", spKey[0], TripleSPOPrefix)
		}
		if opsKey[0] != TripleOPSPrefix {
			t.Errorf("OPS prefix = 0x%02x, want 0x%02x", opsKey[0], TripleOPSPrefix)
		}
		// UnpackTopicID of first 8 bytes should match
		gotTopic := UnpackTopicID(binary.BigEndian.Uint64(spKey[1:]))
		if gotTopic != topicID {
			t.Errorf("SPO topic roundtrip: got %d, want %d", gotTopic, topicID)
		}
		gotTopic = UnpackTopicID(binary.BigEndian.Uint64(opsKey[1:]))
		if gotTopic != topicID {
			t.Errorf("OPS topic roundtrip: got %d, want %d", gotTopic, topicID)
		}
	}
}
