// See the LICENSE file for license details.

package boltdb_test

import (
	"testing"

	"github.com/mxmauro/boltdb/v3"
)

// -----------------------------------------------------------------------------

func TestDecodeUint64ShortInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{name: "empty", input: nil, want: 0},
		{name: "one-byte", input: []byte{0x7f}, want: 0x7f},
		{name: "three-bytes", input: []byte{0x78, 0x56, 0x34}, want: 0x345678},
		{name: "eight-bytes", input: []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, want: 0x0102030405060708},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boltdb.DecodeUint64(tt.input); got != tt.want {
				t.Fatalf("unexpected decoded value [got=%#x want=%#x]", got, tt.want)
			}
		})
	}
}

func TestDecodeUint32ShortInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint32
	}{
		{name: "empty", input: nil, want: 0},
		{name: "one-byte", input: []byte{0x7f}, want: 0x7f},
		{name: "three-bytes", input: []byte{0x78, 0x56, 0x34}, want: 0x345678},
		{name: "four-bytes", input: []byte{0x04, 0x03, 0x02, 0x01}, want: 0x01020304},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boltdb.DecodeUint32(tt.input); got != tt.want {
				t.Fatalf("unexpected decoded value [got=%#x want=%#x]", got, tt.want)
			}
		})
	}
}

func TestDecodeUint16ShortInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint16
	}{
		{name: "empty", input: nil, want: 0},
		{name: "one-byte", input: []byte{0x7f}, want: 0x7f},
		{name: "two-bytes", input: []byte{0x34, 0x12}, want: 0x1234},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boltdb.DecodeUint16(tt.input); got != tt.want {
				t.Fatalf("unexpected decoded value [got=%#x want=%#x]", got, tt.want)
			}
		})
	}
}

func TestEncodeHelpers(t *testing.T) {
	uint64Value := uint64(0x0102030405060708)
	if got := boltdb.EncodeUint64(uint64Value); len(got) != 8 || boltdb.DecodeUint64(got) != uint64Value {
		t.Fatalf("uint64 encode/decode roundtrip failed [got=%#v]", got)
	}

	uint32Value := uint32(0x01020304)
	if got := boltdb.EncodeUint32(uint32Value); len(got) != 4 || boltdb.DecodeUint32(got) != uint32Value {
		t.Fatalf("uint32 encode/decode roundtrip failed [got=%#v]", got)
	}

	uint16Value := uint16(0x0102)
	if got := boltdb.EncodeUint16(uint16Value); len(got) != 2 || boltdb.DecodeUint16(got) != uint16Value {
		t.Fatalf("uint16 encode/decode roundtrip failed [got=%#v]", got)
	}
}
