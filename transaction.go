package boltdb

import (
	"errors"

	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// -----------------------------------------------------------------------------

// TX represents a transaction within the database.
type TX struct {
	db       *DB
	readOnly bool
	tx       *bbolt.Tx
}

// TxOptions specifies a set of options when starting a transaction.
type TxOptions struct {
	ReadOnly bool
}

// WithinTxCallback is a callback to be called after the transaction is initiated.
type WithinTxCallback func(tx *TX) error

// -----------------------------------------------------------------------------

// Commit stores the transaction changes into the database and, on success, ends the operation.
func (tx *TX) Commit() error {
	if tx.readOnly {
		_ = tx.tx.Rollback()
		return nil
	}
	return tx.tx.Commit()
}

// Rollback discards the transaction changes and ends the operation.
func (tx *TX) Rollback() {
	_ = tx.tx.Rollback()
}

// DB gets the database this transaction belongs to.
func (tx *TX) DB() *DB {
	return tx.db
}

// ReadOnly returns if the transaction is writable or not.
func (tx *TX) ReadOnly() bool {
	return tx.readOnly
}

// Bucket returns a bucket on the database. If the transaction is writable and the bucket does not exist,
// this function will try to create it.
func (tx *TX) Bucket(path []byte) (*Bucket, error) {
	var b *bbolt.Bucket

	// Parse path.
	pi, err := newPathIterator(path)
	if err != nil {
		return nil, err
	}

	// Get/create the top bucket.
	pathFragment, lastFragment := pi.fragment()
	if !tx.readOnly {
		b, err = tx.tx.CreateBucketIfNotExists(pathFragment)
		if err != nil {
			return nil, err
		}
	} else {
		b = tx.tx.Bucket(pathFragment)
		if b == nil {
			return nil, ErrBucketNotFound
		}
	}

	// Get/create nested bucket(s) if multiple path fragments.
	bucketName := pathFragment
	for !lastFragment {
		pathFragment, lastFragment = pi.fragment()
		if !tx.readOnly {
			b, err = b.CreateBucketIfNotExists(pathFragment)
			if err != nil {
				return nil, err
			}
		} else {
			b = b.Bucket(pathFragment)
			if b == nil {
				return nil, ErrBucketNotFound
			}
		}

		bucketName = pathFragment
	}

	// Create wrapper.
	bucket := &Bucket{
		tx:   tx,
		name: bucketName,
		b:    b,
	}

	// Done
	return bucket, nil
}

// DeleteBucket removes an existing child bucket from the database including nested buckets and stored keys.
func (tx *TX) DeleteBucket(path []byte) error {
	// Check if TX is writable.
	if tx.readOnly {
		return ErrTxNotWritable
	}

	// Parse path.
	pi, err := newPathIterator(path)
	if err != nil {
		return err
	}

	pathFragment, lastFragment := pi.fragment()
	if lastFragment {
		// If it is the last fragment, then delete a top bucket.
		err = tx.tx.DeleteBucket(pathFragment)
	} else {
		// Else get the nested bucket.
		b := tx.tx.Bucket(pathFragment)
		if b == nil {
			return nil
		}
		for {
			pathFragment, lastFragment = pi.fragment()
			if lastFragment {
				break
			}

			b = b.Bucket(pathFragment)
			if b == nil {
				return nil
			}
		}

		// And delete it.
		err = b.DeleteBucket(pathFragment)
	}

	// Done
	if err != nil && errors.Is(err, bolterrors.ErrBucketNotFound) {
		return nil // Ignore bucket not found errors.
	}
	return err
}
