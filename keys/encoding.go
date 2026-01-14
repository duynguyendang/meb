package keys

import (
	"encoding/binary"
)

// Prefix constants for different index types
const (
	// Triple indices (legacy, kept for backward compatibility)
	SPOPrefix   byte = 0x01 // Subject-Predicate-Object index
	OPSPrefix   byte = 0x02 // Object-Predicate-Subject index
	PSOPrefix   byte = 0x03 // Predicate-Subject-Object index
	ChunkPrefix byte = 0x10 // Content blob storage

	// Quad indices (new for multi-tenancy/RAG contexts)
	QuadSPOGPrefix byte = 0x20 // Subject-Predicate-Object-Graph index
	QuadPOSGPrefix byte = 0x21 // Predicate-Object-Subject-Graph index
	QuadGSPOPrefix byte = 0x22 // Graph-Subject-Predicate-Object index (for lifecycle)

	// System keys (0xFF reserved for system metadata)
	SystemPrefix byte = 0xFF // System metadata prefix (avoids collision with data keys)
)

// Key size constants
const (
	// Component sizes
	PrefixSize = 1 // Size of key prefix byte
	IDSize     = 8 // Size of each uint64 ID component

	// Triple key sizes: prefix(1) + 3*ID(8) = 25 bytes
	TripleKeySize = PrefixSize + 3*IDSize // 25 bytes

	// Quad key sizes: prefix(1) + 4*ID(8) = 33 bytes
	QuadKeySize = PrefixSize + 4*IDSize // 33 bytes

	// Chunk key sizes: prefix(1) + ID(8) = 9 bytes
	ChunkKeySize = PrefixSize + IDSize // 9 bytes

)

// System metadata keys
var KeyFactCount = []byte{SystemPrefix, 0x01} // Stores the total fact count

// Triple encoding format:
// SPO: [prefix(1) | subject(8) | predicate(8) | object(8)] = 25 bytes
// OPS: [prefix(1) | object(8) | predicate(8) | subject(8)] = 25 bytes

// EncodeSPOKey encodes a triple into an SPO (Subject-Predicate-Object) key.
// Uses BigEndian encoding to ensure lexicographic ordering matches numeric ordering.
func EncodeSPOKey(subject, predicate, object uint64) []byte {
	key := make([]byte, TripleKeySize)
	key[0] = SPOPrefix
	binary.BigEndian.PutUint64(key[1:9], subject)
	binary.BigEndian.PutUint64(key[9:17], predicate)
	binary.BigEndian.PutUint64(key[17:25], object)
	return key
}

// EncodeOPSKey encodes a triple into an OPS (Object-Predicate-Subject) key.
// Uses BigEndian encoding to ensure lexicographic ordering matches numeric ordering.
func EncodeOPSKey(subject, predicate, object uint64) []byte {
	key := make([]byte, TripleKeySize)
	key[0] = OPSPrefix
	binary.BigEndian.PutUint64(key[1:9], object)
	binary.BigEndian.PutUint64(key[9:17], predicate)
	binary.BigEndian.PutUint64(key[17:25], subject)
	return key
}

// EncodePSOKey encodes a triple into a PSO (Predicate-Subject-Object) key.
// Uses BigEndian encoding to ensure lexicographic ordering matches numeric ordering.
func EncodePSOKey(subject, predicate, object uint64) []byte {
	key := make([]byte, TripleKeySize)
	key[0] = PSOPrefix
	binary.BigEndian.PutUint64(key[1:9], predicate)
	binary.BigEndian.PutUint64(key[9:17], subject)
	binary.BigEndian.PutUint64(key[17:25], object)
	return key
}

// DecodePSOKey decodes a PSO key back into subject, predicate, object IDs.
func DecodePSOKey(key []byte) (subject, predicate, object uint64) {
	if len(key) < TripleKeySize || key[0] != PSOPrefix {
		return 0, 0, 0
	}
	predicate = binary.BigEndian.Uint64(key[1:9])
	subject = binary.BigEndian.Uint64(key[9:17])
	object = binary.BigEndian.Uint64(key[17:25])
	return
}

// EncodePSOPrefix creates a prefix for PSO range scans with bound values.
func EncodePSOPrefix(predicate, subject uint64) []byte {
	if predicate == 0 {
		return []byte{PSOPrefix}
	}
	if subject == 0 {
		prefix := make([]byte, PrefixSize+IDSize) // 9 bytes
		prefix[0] = PSOPrefix
		binary.BigEndian.PutUint64(prefix[1:9], predicate)
		return prefix
	}
	// Full prefix: predicate + subject
	prefix := make([]byte, PrefixSize+2*IDSize) // 17 bytes
	prefix[0] = PSOPrefix
	binary.BigEndian.PutUint64(prefix[1:9], predicate)
	binary.BigEndian.PutUint64(prefix[9:17], subject)
	return prefix
}

// DecodeSPOKey decodes an SPO key back into subject, predicate, object IDs.
func DecodeSPOKey(key []byte) (subject, predicate, object uint64) {
	if len(key) < TripleKeySize || key[0] != SPOPrefix {
		return 0, 0, 0
	}
	subject = binary.BigEndian.Uint64(key[1:9])
	predicate = binary.BigEndian.Uint64(key[9:17])
	object = binary.BigEndian.Uint64(key[17:25])
	return
}

// DecodeOPSKey decodes an OPS key back into subject, predicate, object IDs.
func DecodeOPSKey(key []byte) (subject, predicate, object uint64) {
	if len(key) < TripleKeySize || key[0] != OPSPrefix {
		return 0, 0, 0
	}
	object = binary.BigEndian.Uint64(key[1:9])
	predicate = binary.BigEndian.Uint64(key[9:17])
	subject = binary.BigEndian.Uint64(key[17:25])
	return
}

// EncodeSPOPrefix creates a prefix for SPO range scans with bound values.
// If subject is 0, only uses the SPO prefix.
// If predicate is also non-zero, includes both subject and predicate.
func EncodeSPOPrefix(subject, predicate uint64) []byte {
	if subject == 0 {
		return []byte{SPOPrefix}
	}
	if predicate == 0 {
		prefix := make([]byte, PrefixSize+IDSize) // 9 bytes
		prefix[0] = SPOPrefix
		binary.BigEndian.PutUint64(prefix[1:9], subject)
		return prefix
	}
	// Full prefix: subject + predicate
	prefix := make([]byte, PrefixSize+2*IDSize) // 17 bytes
	prefix[0] = SPOPrefix
	binary.BigEndian.PutUint64(prefix[1:9], subject)
	binary.BigEndian.PutUint64(prefix[9:17], predicate)
	return prefix
}

// EncodeOPSPrefix creates a prefix for OPS range scans with bound values.
// If object is 0, only uses the OPS prefix.
// If predicate is also non-zero, includes both object and predicate.
func EncodeOPSPrefix(object, predicate uint64) []byte {
	if object == 0 {
		return []byte{OPSPrefix}
	}
	if predicate == 0 {
		prefix := make([]byte, PrefixSize+IDSize) // 9 bytes
		prefix[0] = OPSPrefix
		binary.BigEndian.PutUint64(prefix[1:9], object)
		return prefix
	}
	// Full prefix: object + predicate
	prefix := make([]byte, PrefixSize+2*IDSize) // 17 bytes
	prefix[0] = OPSPrefix
	binary.BigEndian.PutUint64(prefix[1:9], object)
	binary.BigEndian.PutUint64(prefix[9:17], predicate)
	return prefix
}

// EncodeChunkKey creates the key for storing raw content blobs.
// Format: [Prefix(1) | ID(8)] = 9 bytes
func EncodeChunkKey(id uint64) []byte {
	k := make([]byte, ChunkKeySize)
	k[0] = ChunkPrefix
	binary.BigEndian.PutUint64(k[1:], id)
	return k
}

// Quad encoding format:
// SPOG: [prefix(1) | subject(8) | predicate(8) | object(8) | graph(8)] = 33 bytes
// POSG: [prefix(1) | predicate(8) | object(8) | subject(8) | graph(8)] = 33 bytes
// GSPO: [prefix(1) | graph(8) | subject(8) | predicate(8) | object(8)] = 33 bytes

// EncodeQuadKey encodes a quad into a key with the specified prefix.
// The layout depends on the prefix, allowing flexible index ordering.
// Uses BigEndian encoding to ensure lexicographic ordering matches numeric ordering.
func EncodeQuadKey(prefix byte, s, p, o, g uint64) []byte {
	key := make([]byte, QuadKeySize)
	key[0] = prefix

	// Order depends on prefix
	switch prefix {
	case QuadSPOGPrefix:
		binary.BigEndian.PutUint64(key[1:9], s)   // Subject
		binary.BigEndian.PutUint64(key[9:17], p)  // Predicate
		binary.BigEndian.PutUint64(key[17:25], o) // Object
		binary.BigEndian.PutUint64(key[25:33], g) // Graph
	case QuadPOSGPrefix:
		binary.BigEndian.PutUint64(key[1:9], p)   // Predicate
		binary.BigEndian.PutUint64(key[9:17], o)  // Object
		binary.BigEndian.PutUint64(key[17:25], s) // Subject
		binary.BigEndian.PutUint64(key[25:33], g) // Graph
	case QuadGSPOPrefix:
		binary.BigEndian.PutUint64(key[1:9], g)   // Graph
		binary.BigEndian.PutUint64(key[9:17], s)  // Subject
		binary.BigEndian.PutUint64(key[17:25], p) // Predicate
		binary.BigEndian.PutUint64(key[25:33], o) // Object
	default:
		// Default to SPOG ordering
		binary.BigEndian.PutUint64(key[1:9], s)
		binary.BigEndian.PutUint64(key[9:17], p)
		binary.BigEndian.PutUint64(key[17:25], o)
		binary.BigEndian.PutUint64(key[25:33], g)
	}

	return key
}

// DecodeQuadKey decodes a quad key back into subject, predicate, object, graph IDs.
// Automatically detects the prefix and decodes accordingly.
func DecodeQuadKey(key []byte) (s, p, o, g uint64) {
	if len(key) < QuadKeySize {
		return 0, 0, 0, 0
	}

	prefix := key[0]

	switch prefix {
	case QuadSPOGPrefix:
		s = binary.BigEndian.Uint64(key[1:9])
		p = binary.BigEndian.Uint64(key[9:17])
		o = binary.BigEndian.Uint64(key[17:25])
		g = binary.BigEndian.Uint64(key[25:33])
	case QuadPOSGPrefix:
		p = binary.BigEndian.Uint64(key[1:9])
		o = binary.BigEndian.Uint64(key[9:17])
		s = binary.BigEndian.Uint64(key[17:25])
		g = binary.BigEndian.Uint64(key[25:33])
	case QuadGSPOPrefix:
		g = binary.BigEndian.Uint64(key[1:9])
		s = binary.BigEndian.Uint64(key[9:17])
		p = binary.BigEndian.Uint64(key[17:25])
		o = binary.BigEndian.Uint64(key[25:33])
	}

	return
}

// EncodeQuadSPOGPrefix creates a prefix for SPOG range scans with bound values.
// Supports partial bindings for efficient prefix scans.
func EncodeQuadSPOGPrefix(s, p, o, g uint64) []byte {
	// Start with prefix
	prefix := []byte{QuadSPOGPrefix}

	// Add bound components in order: S, P, O, G
	if s != 0 {
		buf := make([]byte, IDSize)
		binary.BigEndian.PutUint64(buf, s)
		prefix = append(prefix, buf...)

		if p != 0 {
			buf = make([]byte, IDSize)
			binary.BigEndian.PutUint64(buf, p)
			prefix = append(prefix, buf...)

			if o != 0 {
				buf = make([]byte, IDSize)
				binary.BigEndian.PutUint64(buf, o)
				prefix = append(prefix, buf...)

				if g != 0 {
					buf = make([]byte, IDSize)
					binary.BigEndian.PutUint64(buf, g)
					prefix = append(prefix, buf...)
				}
			}
		}
	}

	return prefix
}

// EncodeQuadGSPOPrefix creates a prefix for GSPO range scans for graph lifecycle operations.
// Most commonly used with just the graph ID bound: G|...
func EncodeQuadGSPOPrefix(g uint64) []byte {
	if g == 0 {
		return []byte{QuadGSPOPrefix}
	}

	prefix := make([]byte, PrefixSize+IDSize) // 9 bytes
	prefix[0] = QuadGSPOPrefix
	binary.BigEndian.PutUint64(prefix[1:9], g)
	return prefix
}

// EncodeQuadPOSGPrefix creates a prefix for POSG range scans with bound values.
// Supports partial bindings for efficient reverse traversals.
func EncodeQuadPOSGPrefix(p, o, s, g uint64) []byte {
	// Start with prefix
	prefix := []byte{QuadPOSGPrefix}

	// Add bound components in order: P, O, S, G
	if p != 0 {
		buf := make([]byte, IDSize)
		binary.BigEndian.PutUint64(buf, p)
		prefix = append(prefix, buf...)

		if o != 0 {
			buf = make([]byte, IDSize)
			binary.BigEndian.PutUint64(buf, o)
			prefix = append(prefix, buf...)

			if s != 0 {
				buf = make([]byte, IDSize)
				binary.BigEndian.PutUint64(buf, s)
				prefix = append(prefix, buf...)

				if g != 0 {
					buf = make([]byte, IDSize)
					binary.BigEndian.PutUint64(buf, g)
					prefix = append(prefix, buf...)
				}
			}
		}
	}

	return prefix
}
