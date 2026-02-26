package keys

import (
	"encoding/binary"
)

// Prefix constants for quad index types
// MEB v2 uses only 33-byte quad keys (SPOG format)
const (
	// Quad indices for multi-tenancy/RAG contexts
	QuadSPOGPrefix byte = 0x20 // Subject-Predicate-Object-Graph index
	QuadPOSGPrefix byte = 0x21 // Predicate-Object-Subject-Graph index
	QuadGSPOPrefix byte = 0x22 // Graph-Subject-Predicate-Object index (for lifecycle)

	// Content storage
	ChunkPrefix byte = 0x10 // Content blob storage

	// System keys (0xFF reserved for system metadata)
	SystemPrefix byte = 0xFF // System metadata prefix (avoids collision with data keys)
)

// Key size constants
const (
	// Component sizes
	PrefixSize = 1 // Size of key prefix byte
	IDSize     = 8 // Size of each uint64 ID component

	// Quad key size: prefix(1) + 4*ID(8) = 33 bytes
	QuadKeySize = PrefixSize + 4*IDSize // 33 bytes

	// Chunk key size: prefix(1) + ID(8) = 9 bytes
	ChunkKeySize = PrefixSize + IDSize // 9 bytes
)

// System metadata keys
var KeyFactCount = []byte{SystemPrefix, 0x01} // Stores the total fact count

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

// buildQuadPrefix builds a quad prefix from components in order.
// Only non-zero components are added to the prefix for efficient range scans.
func buildQuadPrefix(prefix byte, components ...uint64) []byte {
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

// EncodeQuadSPOGPrefix creates a prefix for SPOG range scans with bound values.
// Supports partial bindings for efficient prefix scans.
func EncodeQuadSPOGPrefix(s, p, o, g uint64) []byte {
	return buildQuadPrefix(QuadSPOGPrefix, s, p, o, g)
}

// EncodeQuadPOSGPrefix creates a prefix for POSG range scans with bound values.
func EncodeQuadPOSGPrefix(p, o, s, g uint64) []byte {
	return buildQuadPrefix(QuadPOSGPrefix, p, o, s, g)
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

// EncodeChunkKey creates the key for storing raw content blobs.
// Format: [Prefix(1) | ID(8)] = 9 bytes
func EncodeChunkKey(id uint64) []byte {
	k := make([]byte, ChunkKeySize)
	k[0] = ChunkPrefix
	binary.BigEndian.PutUint64(k[1:], id)
	return k
}
