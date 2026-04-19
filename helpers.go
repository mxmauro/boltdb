// See the LICENSE file for license details.

package boltdb

import (
	"encoding/binary"
)

// -----------------------------------------------------------------------------

// EncodeUint64 stores an uint64 into a byte array using little-endian format
func EncodeUint64(value uint64) []byte {
	valueBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueBytes[:], value)
	return valueBytes
}

// DecodeUint64 decodes a little-endian uint64 value
func DecodeUint64(valueBytes []byte) uint64 {
	if len(valueBytes) < 8 {
		newValueBytes := make([]byte, 8)
		copy(newValueBytes, valueBytes) // Higher bytes will remain with zeroes
		return binary.LittleEndian.Uint64(newValueBytes)
	}
	return binary.LittleEndian.Uint64(valueBytes)
}

// EncodeUint32 stores an uint32 into a byte array using little-endian format
func EncodeUint32(value uint32) []byte {
	valueBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(valueBytes[:], value)
	return valueBytes
}

// DecodeUint32 decodes a little-endian uint32 value
func DecodeUint32(valueBytes []byte) uint32 {
	if len(valueBytes) < 4 {
		newValueBytes := make([]byte, 4)
		copy(newValueBytes, valueBytes) // Higher bytes will remain with zeroes
		return binary.LittleEndian.Uint32(newValueBytes)
	}
	return binary.LittleEndian.Uint32(valueBytes)
}

// EncodeUint16 stores an uint16 into a byte array using little-endian format
func EncodeUint16(value uint16) []byte {
	valueBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(valueBytes[:], value)
	return valueBytes
}

// DecodeUint16 decodes a little-endian uint16 value
func DecodeUint16(valueBytes []byte) uint16 {
	if len(valueBytes) < 2 {
		newValueBytes := make([]byte, 2)
		copy(newValueBytes, valueBytes) // Higher bytes will remain with zeroes
		return binary.LittleEndian.Uint16(newValueBytes)
	}
	return binary.LittleEndian.Uint16(valueBytes)
}
