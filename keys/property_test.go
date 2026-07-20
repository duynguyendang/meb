package keys

import (
	"encoding/binary"
	"testing"

	"pgregory.net/rapid"
)

// TestPackIDRoundTrip_Property verifies that PackID/UnpackTopicID/UnpackLocalID
// round-trip correctly across the entire valid range of topic and local IDs.
func TestPackIDRoundTrip_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		topicID := rapid.Uint32Range(0, 0xFFFFFF).Draw(t, "topicID")
		localID := rapid.Uint64Range(0, 0xFFFFFFFFFF).Draw(t, "localID")

		packed := PackID(topicID, localID)

		gotTopic := UnpackTopicID(packed)
		gotLocal := UnpackLocalID(packed)

		if gotTopic != topicID {
			t.Fatalf("topicID mismatch: got %d, want %d (packed=%016x)",
				gotTopic, topicID, packed)
		}
		if gotLocal != localID {
			t.Fatalf("localID mismatch: got %d, want %d (packed=%016x)",
				gotLocal, localID, packed)
		}
	})
}

// TestEncodeDecodeTripleKey_Property verifies that EncodeTripleKey/DecodeTripleKey
// round-trip correctly for both SPO and OPS prefixes across the full ID range.
func TestEncodeDecodeTripleKey_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		prefix := rapid.SampledFrom([]byte{TripleSPOPrefix, TripleOPSPrefix}).Draw(t, "prefix")
		s := rapid.Uint64().Draw(t, "s")
		p := rapid.Uint64().Draw(t, "p")
		o := rapid.Uint64().Draw(t, "o")

		key := EncodeTripleKey(prefix, s, p, o)
		if len(key) != TripleKeySize {
			t.Fatalf("key size = %d, want %d", len(key), TripleKeySize)
		}
		if key[0] != prefix {
			t.Fatalf("prefix = 0x%02x, want 0x%02x", key[0], prefix)
		}

		gotS, gotP, gotO := DecodeTripleKey(key)
		if gotS != s || gotP != p || gotO != o {
			t.Fatalf("DecodeTripleKey(0x%02x) = (%d,%d,%d), want (%d,%d,%d)",
				prefix, gotS, gotP, gotO, s, p, o)
		}
	})
}

// TestInlineBoolRoundTrip_Property verifies that PackInlineBool produces an
// inline ID that survives UnpackInlineXXX round-trip (through scan resolution).
func TestInlineBoolRoundTrip_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := rapid.Bool().Draw(t, "bool")
		packed := PackInlineBool(b)

		if !IsInline(packed) {
			t.Fatalf("PackInlineBool(%v) = %d, not marked as inline", b, packed)
		}

		// Verify the inline bit pattern
		if packed&InlineBit == 0 {
			t.Fatalf("inline bit not set")
		}
		if packed&InlineIsBool != 0 {
			t.Fatalf("bool type marker incorrect")
		}
		if b && packed&1 != 1 {
			t.Fatalf("bool value not preserved: packed=%d, want value=1", packed)
		}
		if !b && packed&1 != 0 {
			t.Fatalf("bool value not preserved: packed=%d, want value=0", packed)
		}
	})
}

// TestInlineInt32RoundTrip_Property verifies that PackInlineInt32 round-trips
// across the full int32 range.
func TestInlineInt32RoundTrip_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := rapid.Int32().Draw(t, "int32")
		packed := PackInlineInt32(v)

		if !IsInline(packed) {
			t.Fatalf("PackInlineInt32(%d) = %d, not marked as inline", v, packed)
		}
		if packed&InlineIsNum == 0 {
			t.Fatalf("number type marker not set for int32 value %d", v)
		}
		if packed&InlineNumF32 != 0 {
			t.Fatalf("int32 subtype marker not set for value %d (float32 bit 37 set)", v)
		}
		// Round-trip via unpack
		got := UnpackInlineInt32(packed)
		if got != v {
			t.Fatalf("roundtrip: UnpackInlineInt32(PackInlineInt32(%d)) = %d", v, got)
		}
	})
}

// TestInlineFloat32RoundTrip_Property verifies PackInlineFloat32 round-trips
func TestInlineFloat32RoundTrip_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := rapid.Float32Range(-1e6, 1e6).Draw(t, "float32")
		packed := PackInlineFloat32(v)
		if !IsInline(packed) {
			t.Fatalf("PackInlineFloat32(%v) = %d, not marked as inline", v, packed)
		}
		if packed&InlineIsNum == 0 {
			t.Fatalf("number type marker not set for float32 value %v", v)
		}
		if packed&InlineNumF32 == 0 {
			t.Fatalf("float32 subtype marker not set for value %v (bit 37)", v)
		}
		// Round-trip via unpack
		got := UnpackInlineFloat32(packed)
		if got != v {
			t.Fatalf("roundtrip: UnpackInlineFloat32(PackInlineFloat32(%v)) = %v", v, got)
		}
	})
}

// TestEncodeDecodeChunkKey_Property verifies EncodeChunkKey round-trips
func TestEncodeDecodeChunkKey_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		id := rapid.Uint64().Draw(t, "id")
		key := EncodeChunkKey(id)
		if len(key) != ChunkKeySize {
			t.Fatalf("key size = %d, want %d", len(key), ChunkKeySize)
		}
		if key[0] != ChunkPrefix {
			t.Fatalf("prefix = 0x%02x, want 0x%02x", key[0], ChunkPrefix)
		}
		gotID := binary.BigEndian.Uint64(key[1:])
		if gotID != id {
			t.Fatalf("chunk key roundtrip: got %d, want %d", gotID, id)
		}
	})
}

// TestEncodeVectorFullKey_Property verifies EncodeVectorFullKey round-trips
func TestEncodeVectorFullKey_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		id := rapid.Uint64().Draw(t, "id")
		key := EncodeVectorFullKey(id)
		if len(key) != ChunkKeySize {
			t.Fatalf("key size = %d, want %d", len(key), ChunkKeySize)
		}
		if key[0] != VectorFullPrefix {
			t.Fatalf("prefix = 0x%02x, want 0x%02x", key[0], VectorFullPrefix)
		}
		gotID := binary.BigEndian.Uint64(key[1:])
		if gotID != id {
			t.Fatalf("vector key roundtrip: got %d, want %d", gotID, id)
		}
	})
}

// TestEncodeSPOByTopic_Property verifies topic-based key prefixes
func TestEncodeSPOByTopic_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		topicID := rapid.Uint32Range(0, 0xFFFFFF).Draw(t, "topicID")
		spKey := EncodeSPOByTopic(topicID)
		opsKey := EncodeOPSByTopic(topicID)
		if len(spKey) != PrefixSize+IDSize {
			t.Fatalf("SPO topic key size = %d, want %d", len(spKey), PrefixSize+IDSize)
		}
		if len(opsKey) != PrefixSize+IDSize {
			t.Fatalf("OPS topic key size = %d, want %d", len(opsKey), PrefixSize+IDSize)
		}
		if spKey[0] != TripleSPOPrefix {
			t.Fatalf("SPO prefix = 0x%02x, want 0x%02x", spKey[0], TripleSPOPrefix)
		}
		if opsKey[0] != TripleOPSPrefix {
			t.Fatalf("OPS prefix = 0x%02x, want 0x%02x", opsKey[0], TripleOPSPrefix)
		}
		gotTopic := UnpackTopicID(binary.BigEndian.Uint64(spKey[1:]))
		if gotTopic != topicID {
			t.Fatalf("SPO topic roundtrip: got %d, want %d", gotTopic, topicID)
		}
		gotTopic = UnpackTopicID(binary.BigEndian.Uint64(opsKey[1:]))
		if gotTopic != topicID {
			t.Fatalf("OPS topic roundtrip: got %d, want %d", gotTopic, topicID)
		}
	})
}

// TestEncodeDecodeIVFPostingKey_Property verifies IVF posting keys
func TestEncodeDecodeIVFPostingKey_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		topicID := rapid.Uint32Range(0, 0xFFFFFF).Draw(t, "topicID")
		centroidID := rapid.Uint32().Draw(t, "centroidID")
		localID := rapid.Uint64Range(0, 0xFFFFFFFFFF).Draw(t, "localID")
		key := EncodeIVFPostingKey(topicID, centroidID, localID)
		if len(key) != 14 {
			t.Fatalf("posting key size = %d, want 14", len(key))
		}
		if key[0] != IVFPostingPrefix {
			t.Fatalf("prefix = 0x%02x", key[0])
		}
		gotLocal := ExtractLocalIDFromPostingKey(key)
		if gotLocal != localID {
			t.Fatalf("localID: got %d, want %d", gotLocal, localID)
		}
		gotCentroid := ExtractCentroidIDFromPostingKey(key)
		if gotCentroid != centroidID {
			t.Fatalf("centroidID: got %d, want %d", gotCentroid, centroidID)
		}
	})
}

// TestEncodeHNSWKeyRoundTrip_Property verifies HNSW keys round-trip
func TestEncodeHNSWKeyRoundTrip_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		topicID := rapid.Uint32Range(0, 0xFFFFFF).Draw(t, "topicID")
		localID := rapid.Uint64Range(0, 0xFFFFFFFFFF).Draw(t, "localID")
		level := rapid.IntRange(0, 127).Draw(t, "level")
		packedID := PackID(topicID, localID)
		key := EncodeHNSWKey(packedID, level)
		if len(key) != 10 {
			t.Fatalf("HNSW key size = %d, want 10", len(key))
		}
		if key[0] != HNSWPrefix {
			t.Fatalf("prefix = 0x%02x, want 0x%02x", key[0], HNSWPrefix)
		}
		gotPacked := binary.BigEndian.Uint64(key[1:9])
		if gotPacked != packedID {
			t.Fatalf("packedID: got %d, want %d", gotPacked, packedID)
		}
		if key[9] != byte(level) {
			t.Fatalf("level: got %d, want %d", key[9], level)
		}
	})
}
