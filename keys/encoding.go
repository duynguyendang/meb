package keys

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

const (
	TripleSPOPrefix byte = 0x20
	TripleOPSPrefix byte = 0x21

	ChunkPrefix      byte = 0x10
	VectorFullPrefix byte = 0x11
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

// Inline ID encoding: store primitive values directly in the 64-bit object ID.
//
// Bit layout:
//
//	bit 39     = 1  (inline flag)
//	bit 38     = type (0=bool, 1=number)
//	bits 37-0  = payload (38 bits — fits zigzag int32 [38 bits] or IEEE float32 [32 bits])
//
// For number type, payload[37] distinguishes: 0=int32(zigzag), 1=float32
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
func PackID(topicID uint32, localID uint64) uint64 {
	return (uint64(topicID) << TopicIDShift) | (localID & LocalIDMask)
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
func NextTopicID(currentID uint64) uint64 {
	return (currentID & TopicIDMask) + (1 << TopicIDShift)
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
		binary.BigEndian.PutUint64(key[1:9], s)
		binary.BigEndian.PutUint64(key[9:17], p)
		binary.BigEndian.PutUint64(key[17:25], o)
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

// EncodeTripleSPOPrefix builds a partial SPO prefix for scan operations.
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
