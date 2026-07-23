package keys

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
)

const (
	TripleSPOPrefix byte = 0x20 // SPO: subject || predicate || object
	TripleOPSPrefix byte = 0x21 // OPS: object || predicate || subject

	ChunkPrefix      byte = 0x10
	VectorFullPrefix byte = 0x11
	HNSWPrefix       byte = 0x12

	IVFCentroidPrefix  byte = 0x13
	IVFPostingPrefix   byte = 0x14
	IVFCodebookPrefix  byte = 0x15
	IVFRawVectorPrefix byte = 0x16

	ForwardDictPrefix byte = 0x80
	ReverseDictPrefix byte = 0x81

	SystemPrefix     byte = 0xFF
)

const (
	PrefixSize    = 1
	IDSize        = 8
	TripleKeySize = PrefixSize + 3*IDSize
	ChunkKeySize  = PrefixSize + IDSize
)

// TopicID bit-packing constants.
// ID = (TopicID << 40) | LocalID
const (
	TopicIDBits  = 24
	LocalIDBits  = 40
	TopicIDShift = 64 - TopicIDBits // 40
	TopicIDMask  = uint64(0xFFFFFF) << TopicIDShift
	LocalIDMask  = (uint64(1) << LocalIDBits) - 1 // 0xFFFFFFFFFF
)

// Semantic Hints constants (upper 16 bits of Content_Offset).
const (
	EntityTypeBits   = 4
	SemanticHashBits = 8
	FlagsBits        = 4

	EntityTypeShift   = 12 // bits 15-12 (maps to bits 63-60 of 64-bit value)
	SemanticHashShift = 4  // bits 11-4
	FlagsShift        = 0  // bits 3-0
)

// EntityType values (4-bit).
const (
	EntityUnknown   uint16 = 0
	EntityFunc      uint16 = 1
	EntityVar       uint16 = 2
	EntityClass     uint16 = 3
	EntityModule    uint16 = 4
	EntityInterface uint16 = 5
	EntityType      uint16 = 6
	EntityConst     uint16 = 7
	EntityPackage   uint16 = 8
)

// Flags bit definitions (4-bit).
const (
	FlagIsPublic     uint16 = 0x8 // bit 3
	FlagIsDeprecated uint16 = 0x4 // bit 2
	FlagIsTest       uint16 = 0x2 // bit 1
	FlagIsGenerated  uint16 = 0x1 // bit 0
)

var KeyFactCount = []byte{SystemPrefix, 0x01}

// Schema version marker stored in BadgerDB to detect data format version.
// Increment when bit layouts change (Inline ID, TopicID, Semantic Hints).
var KeySchemaVersion = []byte{SystemPrefix, 0x02}

const CurrentSchemaVersion = 2

const (
	snapshotVectorChunk byte = 0x10
	snapshotIDChunk     byte = 0x11
	snapshotMeta        byte = 0x12
	snapshotDirty       byte = 0x13
)

var KeySnapshotDirty = []byte{SystemPrefix, snapshotDirty}

func EncodeSnapshotVectorChunkKey(chunkIdx int) []byte {
	key := make([]byte, 6)
	key[0] = SystemPrefix
	key[1] = snapshotVectorChunk
	binary.BigEndian.PutUint32(key[2:], uint32(chunkIdx))
	return key
}

func EncodeSnapshotIDChunkKey(chunkIdx int) []byte {
	key := make([]byte, 6)
	key[0] = SystemPrefix
	key[1] = snapshotIDChunk
	binary.BigEndian.PutUint32(key[2:], uint32(chunkIdx))
	return key
}

var KeySnapshotMeta = []byte{SystemPrefix, snapshotMeta}

// Inline ID encoding: store primitive values directly in the 64-bit object ID.
//
// Bit layout:
//
//	bit 39     = 1  (inline flag)
//	bit 38     = type (0=bool, 1=number)
//	bits 37-0  = payload (38 bits — fits zigzag int32 [38 bits] or IEEE float32 [32 bits])
//
// For number type, payload[37] distinguishes: 0=int32(zigzag), 1=float32
//
// These constants are exported for use by the inline encoding functions.
// They are internal implementation details of the ID encoding scheme and
// should not be used directly by external callers.
const (
	InlineBit      = uint64(1) << 39
	InlineIsBool   = uint64(0) << 38       // bit 38 = 0: bool, bit 0 = value
	InlineIsNum    = uint64(1) << 38       // bit 38 = 1: number, bit 37 = 0: int32, 1: float32
	InlineNumI32   = uint64(0) << 37       // bit 37 = 0: int32 zigzag in bits 36-0
	InlineNumF32   = uint64(1) << 37       // bit 37 = 1: float32 IEEE in bits 31-0
	InlinePayload  = (uint64(1) << 37) - 1 // bits 36-0 for int32 zigzag
	InlineFPayload = (uint64(1) << 32) - 1 // bits 31-0 for float32
)

// IsInline returns true if the ID encodes an inline primitive value.
func IsInline(id uint64) bool {
	return (id & InlineBit) != 0
}

// PackInlineBool encodes a bool as an inline ID.
func PackInlineBool(v bool) uint64 {
	payload := uint64(0)
	if v {
		payload = 1
	}
	return InlineBit | InlineIsBool | payload
}

// PackInlineInt32 encodes an int32 as an inline ID.
// Uses offset encoding (add 2^31 to map [-2^31, 2^31-1] to [0, 2^32-1]).
func PackInlineInt32(v int32) uint64 {
	offset := uint64(uint32(v)) // reinterpret bits as unsigned (same as adding 2^31)
	return InlineBit | InlineIsNum | InlineNumI32 | (offset & InlinePayload)
}

// PackInlineFloat32 encodes a float32 as an inline ID.
func PackInlineFloat32(v float32) uint64 {
	bits := uint64(math.Float32bits(v))
	return InlineBit | InlineIsNum | InlineNumF32 | (bits & InlineFPayload)
}

// UnpackInlineBool decodes a bool from an inline ID.
func UnpackInlineBool(id uint64) bool {
	return (id & 1) != 0
}

// UnpackInlineInt32 decodes an int32 from an inline ID.
func UnpackInlineInt32(id uint64) int32 {
	return int32(id & InlinePayload)
}

// UnpackInlineFloat32 decodes a float32 from an inline ID.
func UnpackInlineFloat32(id uint64) float32 {
	bits := uint32(id & InlineFPayload)
	return math.Float32frombits(bits)
}

// PackID combines a 24-bit TopicID and 40-bit LocalID into a single 64-bit ID.
// ID = (TopicID << 40) | LocalID
// Callers must ensure topicID fits in 24 bits and localID fits in 40 bits.
// Out-of-range values are silently masked to avoid data corruption.
func PackID(topicID uint32, localID uint64) uint64 {
	return (uint64(topicID) << TopicIDShift) | (localID & LocalIDMask)
}

// ValidatePackedIDComponents checks that topicID and localID fit in their
// respective bit widths. Returns an error if either overflows.
func ValidatePackedIDComponents(topicID uint32, localID uint64) error {
	if topicID != (topicID & (1<<TopicIDBits - 1)) {
		return fmt.Errorf("topicID %d exceeds %d bits", topicID, TopicIDBits)
	}
	if localID != (localID & LocalIDMask) {
		return fmt.Errorf("localID %d exceeds %d bits", localID, LocalIDBits)
	}
	return nil
}

// UnpackTopicID extracts the 24-bit TopicID from a packed 64-bit ID.
func UnpackTopicID(id uint64) uint32 {
	return uint32((id & TopicIDMask) >> TopicIDShift)
}

// UnpackLocalID extracts the 40-bit LocalID from a packed 64-bit ID.
func UnpackLocalID(id uint64) uint64 {
	return id & LocalIDMask
}

// TopicMask returns a mask for comparing TopicID portions of IDs.
func TopicMask() uint64 {
	return TopicIDMask
}

// NextTopicID returns the first ID in the next TopicID range.
// Used by LFTJ to leapfrog over unrelated topics.
// Returns math.MaxUint64 if currentID is already at the maximum topic boundary
// to prevent wrapping.
func NextTopicID(currentID uint64) uint64 {
	next := (currentID & TopicIDMask) + (1 << TopicIDShift)
	// Saturate instead of wrapping when topicID would overflow 24 bits.
	if next < currentID {
		return math.MaxUint64
	}
	return next
}

// EncodeSemanticHints packs EntityType, SemanticHash, and Flags into a 16-bit value.
func EncodeSemanticHints(entityType, semanticHash, flags uint16) uint16 {
	return (entityType&0xF)<<EntityTypeShift |
		(semanticHash&0xFF)<<SemanticHashShift |
		(flags & 0xF)
}

// DecodeSemanticHints extracts EntityType, SemanticHash, and Flags from a 16-bit value.
func DecodeSemanticHints(hints uint16) (entityType, semanticHash, flags uint16) {
	entityType = (hints >> EntityTypeShift) & 0xF
	semanticHash = (hints >> SemanticHashShift) & 0xFF
	flags = hints & 0xF
	return
}

// HashSemanticName computes an 8-bit FNV-1a hash for a string name.
func HashSemanticName(name string) uint8 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return uint8(h.Sum32() & 0xFF)
}

func EncodeTripleKey(prefix byte, s, p, o uint64) []byte {
	key := make([]byte, TripleKeySize)
	key[0] = prefix

	switch prefix {
	case TripleSPOPrefix:
		binary.BigEndian.PutUint64(key[1:9], s)
		binary.BigEndian.PutUint64(key[9:17], p)
		binary.BigEndian.PutUint64(key[17:25], o)
	case TripleOPSPrefix:
		binary.BigEndian.PutUint64(key[1:9], o)
		binary.BigEndian.PutUint64(key[9:17], p)
		binary.BigEndian.PutUint64(key[17:25], s)
	default:
		panic(fmt.Sprintf("EncodeTripleKey: unknown prefix 0x%02x", prefix))
	}

	return key
}

func DecodeTripleKey(key []byte) (s, p, o uint64) {
	if len(key) < TripleKeySize {
		return 0, 0, 0
	}

	prefix := key[0]

	switch prefix {
	case TripleSPOPrefix:
		s = binary.BigEndian.Uint64(key[1:9])
		p = binary.BigEndian.Uint64(key[9:17])
		o = binary.BigEndian.Uint64(key[17:25])
	case TripleOPSPrefix:
		o = binary.BigEndian.Uint64(key[1:9])
		p = binary.BigEndian.Uint64(key[9:17])
		s = binary.BigEndian.Uint64(key[17:25])
	default:
		return 0, 0, 0
	}

	return
}

// buildTriplePrefix builds a partial prefix for scan operations.
// A zero component means "unbound" — the prefix is truncated at that position
// so it matches all values for subsequent components. This is safe because
// packed IDs with topicID ≥ 1 are always non-zero, and dictionary IDs
// are allocated starting from 1.
func buildTriplePrefix(prefix byte, components ...uint64) []byte {
	result := []byte{prefix}
	buf := make([]byte, IDSize)

	for _, comp := range components {
		if comp == 0 {
			break
		}
		binary.BigEndian.PutUint64(buf, comp)
		result = append(result, buf...)
	}

	return result
}

func EncodeTripleSPOPrefix(s, p, o uint64) []byte {
	return buildTriplePrefix(TripleSPOPrefix, s, p, o)
}

func EncodeTripleOPSPrefix(o, p, s uint64) []byte {
	return buildTriplePrefix(TripleOPSPrefix, o, p, s)
}

// EncodeSPOByTopic builds a prefix that scans all triples for a given TopicID in SPO index.
func EncodeSPOByTopic(topicID uint32) []byte {
	packedTopic := uint64(topicID) << TopicIDShift
	buf := make([]byte, PrefixSize+IDSize)
	buf[0] = TripleSPOPrefix
	binary.BigEndian.PutUint64(buf[1:], packedTopic)
	return buf
}

// EncodeOPSByTopic builds a prefix that scans all triples for a given TopicID in OPS index.
func EncodeOPSByTopic(topicID uint32) []byte {
	packedTopic := uint64(topicID) << TopicIDShift
	buf := make([]byte, PrefixSize+IDSize)
	buf[0] = TripleOPSPrefix
	binary.BigEndian.PutUint64(buf[1:], packedTopic)
	return buf
}

func EncodeChunkKey(id uint64) []byte {
	k := make([]byte, ChunkKeySize)
	k[0] = ChunkPrefix
	binary.BigEndian.PutUint64(k[1:], id)
	return k
}

func EncodeVectorFullKey(id uint64) []byte {
	k := make([]byte, ChunkKeySize)
	k[0] = VectorFullPrefix
	binary.BigEndian.PutUint64(k[1:], id)
	return k
}

// EncodeIVFRawVectorKey returns the key for a raw float32 vector in the IVF-PQ index.
func EncodeIVFRawVectorKey(topicID uint32, localID uint64) []byte {
	k := make([]byte, ChunkKeySize)
	k[0] = IVFRawVectorPrefix
	binary.BigEndian.PutUint64(k[1:], PackID(topicID, localID))
	return k
}

// TripleValueSize is the size of a triple value in bytes (exported for pools).
const TripleValueSize = 16

// TripleKeyPool provides reusable 25-byte triple key buffers.
var TripleKeyPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, TripleKeySize)
		return &b
	},
}

// TripleValuePool provides reusable 16-byte triple value buffers.
var TripleValuePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, TripleValueSize)
		return &b
	},
}

// EncodeTripleKeyInto writes the encoded triple key into the provided buffer
// and returns the buffer. The buffer must be at least TripleKeySize bytes.
func EncodeTripleKeyInto(buf []byte, prefix byte, s, p, o uint64) []byte {
	buf = buf[:TripleKeySize]
	buf[0] = prefix

	switch prefix {
	case TripleSPOPrefix:
		binary.BigEndian.PutUint64(buf[1:9], s)
		binary.BigEndian.PutUint64(buf[9:17], p)
		binary.BigEndian.PutUint64(buf[17:25], o)
	case TripleOPSPrefix:
		binary.BigEndian.PutUint64(buf[1:9], o)
		binary.BigEndian.PutUint64(buf[9:17], p)
		binary.BigEndian.PutUint64(buf[17:25], s)
	default:
		panic(fmt.Sprintf("EncodeTripleKeyInto: unknown prefix 0x%02x", prefix))
	}

	return buf
}

// EncodeTripleValueWithHintsInto writes the encoded triple value into the provided buffer
// and returns the buffer. The buffer must be at least TripleValueSize bytes.
func EncodeTripleValueWithHintsInto(buf []byte, vectorID uint64, contentOffset uint64, hints uint16) []byte {
	buf = buf[:TripleValueSize]
	packed := (uint64(hints) << 48) | (contentOffset & ((1 << 48) - 1))
	binary.BigEndian.PutUint64(buf[0:8], vectorID)
	binary.BigEndian.PutUint64(buf[8:16], packed)
	return buf
}

// --- IVF-PQ Key Encoding ---

// EncodeIVFCentroidKey builds a 9-byte key:
// [0x13][TopicID:32][CentroidID:32]
// Note: TopicID uses 4 bytes for alignment, though only 24 bits are significant.
func EncodeIVFCentroidKey(topicID uint32, centroidID uint32) []byte {
	buf := make([]byte, 9)
	buf[0] = IVFCentroidPrefix
	binary.BigEndian.PutUint32(buf[1:5], topicID)
	binary.BigEndian.PutUint32(buf[5:9], centroidID)
	return buf
}

// EncodeIVFPostingKey builds a 14-byte key:
// [0x14][TopicID:32][CentroidID:32][LocalID:40]
// Note: TopicID uses 4 bytes for alignment, though only 24 bits are significant.
func EncodeIVFPostingKey(topicID uint32, centroidID uint32, localID uint64) []byte {
	buf := make([]byte, 14)
	buf[0] = IVFPostingPrefix
	binary.BigEndian.PutUint32(buf[1:5], topicID)
	binary.BigEndian.PutUint32(buf[5:9], centroidID)
	buf[9] = byte(localID >> 32)
	binary.BigEndian.PutUint32(buf[10:14], uint32(localID))
	return buf
}

// IVFPostingPrefixByTopic returns the prefix to scan all postings for a topic:
// [0x14][TopicID:24] — 5 bytes
func IVFPostingPrefixByTopic(topicID uint32) []byte {
	buf := make([]byte, 5)
	buf[0] = IVFPostingPrefix
	binary.BigEndian.PutUint32(buf[1:5], topicID)
	return buf
}

// IVFPostingPrefixByCentroid returns the prefix to scan all postings for a centroid:
// [0x14][TopicID:24][CentroidID:4] — 9 bytes
func IVFPostingPrefixByCentroid(topicID uint32, centroidID uint32) []byte {
	return EncodeIVFPostingKey(topicID, centroidID, 0)[:9]
}

// ExtractLocalIDFromPostingKey extracts the 40-bit LocalID from a posting key.
func ExtractLocalIDFromPostingKey(key []byte) uint64 {
	if len(key) < 14 {
		return 0
	}
	return (uint64(key[9]) << 32) | uint64(binary.BigEndian.Uint32(key[10:14]))
}

// ExtractCentroidIDFromPostingKey extracts the 32-bit CentroidID from a posting key.
func ExtractCentroidIDFromPostingKey(key []byte) uint32 {
	if len(key) < 9 {
		return 0
	}
	return binary.BigEndian.Uint32(key[5:9])
}

// --- HNSW Key Encoding ---

// EncodeHNSWKey builds a 10-byte HNSW key: [0x12][packedNodeID:8][level:1]
func EncodeHNSWKey(packedNodeID uint64, level int) []byte {
	buf := make([]byte, 10)
	buf[0] = HNSWPrefix
	binary.BigEndian.PutUint64(buf[1:9], packedNodeID)
	buf[9] = byte(level)
	return buf
}

// EncodeHNSWTombstoneKey builds a 10-byte tombstone key: [0x12][0xFF][packedNodeID:8]
func EncodeHNSWTombstoneKey(packedNodeID uint64) []byte {
	buf := make([]byte, 10)
	buf[0] = HNSWPrefix
	buf[1] = 0xFF // tombstone marker
	binary.BigEndian.PutUint64(buf[2:10], packedNodeID)
	return buf
}

// HNSWTopicPrefix returns the topic-scoped prefix for prefix-scan.
// HNSW key = [0x12][packedNodeID:8][level:1]. packedNodeID = PackID(topicID, localID)
// where topicID is in the upper 24 bits (bytes 0-2 of the uint64 in big-endian).
// Prefix = [0x12][topicID bytes 0-2] = 4 bytes.
func HNSWTopicPrefix(topicID uint32) []byte {
	buf := make([]byte, 4)
	buf[0] = HNSWPrefix
	buf[1] = byte(topicID >> 16)
	buf[2] = byte(topicID >> 8)
	buf[3] = byte(topicID)
	return buf
}

// HNSWNodePrefix returns the prefix for all levels of a node:
// [0x12][packedNodeID:8] (9 bytes)
func HNSWNodePrefix(packedNodeID uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = HNSWPrefix
	binary.BigEndian.PutUint64(buf[1:9], packedNodeID)
	return buf
}
