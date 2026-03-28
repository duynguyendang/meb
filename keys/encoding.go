package keys

import (
	"encoding/binary"
)

const (
	TripleSPOPrefix byte = 0x20
	TripleOPSPrefix byte = 0x21

	ChunkPrefix  byte = 0x10
	SystemPrefix byte = 0xFF
)

const (
	PrefixSize    = 1
	IDSize        = 8
	TripleKeySize = PrefixSize + 3*IDSize
	ChunkKeySize  = PrefixSize + IDSize
)

var KeyFactCount = []byte{SystemPrefix, 0x01}

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
	}

	return
}

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

func EncodeChunkKey(id uint64) []byte {
	k := make([]byte, ChunkKeySize)
	k[0] = ChunkPrefix
	binary.BigEndian.PutUint64(k[1:], id)
	return k
}
