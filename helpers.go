// See the LICENSE file for license details.

package boltdb

import (
	"bytes"
	"encoding/binary"
)

// -----------------------------------------------------------------------------

// EncodeUint64 stores an uint64 into a byte array using little endian format
func EncodeUint64(value uint64) []byte {
	valueBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueBytes[:], value)
	return valueBytes
}

// DecodeUint64 decodes a little endian uint64 value
func DecodeUint64(valueBytes []byte) uint64 {
	if len(valueBytes) < 8 {
		newValueBytes := make([]byte, 8)
		copy(newValueBytes, valueBytes) // Higher bytes will remain with zeroes
		return binary.LittleEndian.Uint64(valueBytes)

	}
	return binary.LittleEndian.Uint64(valueBytes)
}

// -----------------------------------------------------------------------------

func removeLeadingSlashes(path []byte) []byte {
	var ofs int
	for ofs = 0; ofs < len(path) && path[ofs] == '/'; ofs++ {
	}
	return path[ofs:]
}

func removeTrailingSlashes(path []byte) []byte {
	var ofs int
	for ofs = len(path); ofs > 0 && path[ofs-1] == '/'; ofs-- {
	}
	return path[:ofs]
}

func getPathFragmentLen(path []byte) int {
	ofs := bytes.IndexByte(path, '/')
	if ofs < 0 {
		ofs = len(path)
	}
	return ofs
}
