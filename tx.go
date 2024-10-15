// See the LICENSE file for license details.

package boltdb

import (
	"bytes"
	"errors"

	"go.etcd.io/bbolt"
)

// -----------------------------------------------------------------------------

// TX represents a read-write or read-only transaction in BoltDB
type TX struct {
	db       *DB
	readOnly bool
	tx       *bbolt.Tx
}

// WithTxCallback is a callback to be called after the transaction is initiated
type WithTxCallback func(tx *TX) error

// -----------------------------------------------------------------------------

// DB gets the database this transaction belongs to
func (tx *TX) DB() *DB {
	return tx.db
}

// ReadOnly returns if the transaction is read-write or read-only
func (tx *TX) ReadOnly() bool {
	return tx.readOnly
}

// Bucket returns a bucket on the database (and creates if it does not exist)
func (tx *TX) Bucket(path []byte) (*Bucket, error) {
	var b *bbolt.Bucket

	path = removeLeadingSlashes(path)
	nameLen := getPathFragmentLen(path)
	if nameLen < 1 {
		return nil, ErrInvalidPath
	}

	if !tx.readOnly {
		var err error

		b, err = tx.tx.CreateBucketIfNotExists(path[0:nameLen])
		if err != nil {
			return nil, err
		}
	} else {
		b = tx.tx.Bucket(path[0:nameLen])
		if b == nil {
			return nil, ErrBucketNotFound
		}
	}

	// Initialize bucket object
	bucket := &Bucket{
		tx:   tx,
		name: path[0:nameLen],
		b:    b,
	}

	// Get child bucket if requested
	path = removeLeadingSlashes(path[nameLen:])
	if len(path) > 0 {
		var err error

		bucket, err = bucket.Bucket(path)
		if err != nil {
			return nil, err
		}
	}

	// Done
	return bucket, nil
}

// DeleteBucket removes an existing child bucket on the database
// NOTE: Inner sub-keys and buckets will be also deleted
func (tx *TX) DeleteBucket(path []byte) error {
	var err error

	if tx.readOnly {
		return ErrTxNotWritable
	}

	path = removeLeadingSlashes(removeTrailingSlashes(path))
	lastSlash := bytes.LastIndexByte(path, '/')
	if lastSlash < 0 {
		err = tx.tx.DeleteBucket(path)
	} else {
		var bucket *Bucket

		bucket, err = tx.Bucket(path[0:lastSlash])
		if err == nil {
			err = bucket.DeleteBucket(path[lastSlash+1:])
		}
	}

	// Ignore bucket not found errors
	if err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
		return err
	}
	return nil
}
