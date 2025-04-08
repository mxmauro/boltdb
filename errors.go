// See the LICENSE file for license details.

package boltdb

import (
	"errors"

	"go.etcd.io/bbolt"
)

// -----------------------------------------------------------------------------

var (
	ErrInvalidPath           = errors.New("invalid path")
	ErrBucketNotFound        = bbolt.ErrBucketNotFound
	ErrTxNotWritable         = bbolt.ErrTxNotWritable
	ErrDatabaseReadOnly      = bbolt.ErrDatabaseReadOnly
	ErrInvalidCursorPosition = errors.New("invalid cursor position")
)
