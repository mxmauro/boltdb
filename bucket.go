// See the LICENSE file for license details.

package boltdb

import (
	"bytes"
	"errors"

	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// -----------------------------------------------------------------------------

// Bucket represents a directory that contains keys and values inside the database.
type Bucket struct {
	tx   *TX
	name []byte
	b    *bbolt.Bucket
}

// BucketStats contains statistical data about a bucket.
type BucketStats = bbolt.BucketStats

// -----------------------------------------------------------------------------

// DB gets the database associated with this bucket.
func (bucket *Bucket) DB() *DB {
	return bucket.tx.db
}

// TX gets the transaction associated with this bucket.
func (bucket *Bucket) TX() *TX {
	return bucket.tx
}

// Name returns the bucket name.
func (bucket *Bucket) Name() []byte {
	return bucket.name
}

// NextSequence returns an autoincrement integer for the bucket.
func (bucket *Bucket) NextSequence() (uint64, error) {
	return bucket.b.NextSequence()
}

// Get returns the value of a key in a bucket or nil if not found.
func (bucket *Bucket) Get(key []byte) []byte {
	return bucket.b.Get(key)
}

// Put stores a key/value pair in the bucket.
func (bucket *Bucket) Put(key []byte, value []byte) error {
	return bucket.b.Put(key, value)
}

// Delete deletes a specific key. No error is returned if key is not found.
func (bucket *Bucket) Delete(key []byte) error {
	return bucket.b.Delete(key)
}

// Bucket returns a bucket on the database (and creates if it does not exist)
func (bucket *Bucket) Bucket(path []byte) (*Bucket, error) {
	// Parse path.
	pi, err := newPathIterator(path)
	if err != nil {
		return nil, err
	}

	// Get nested bucket.
	b := bucket.b
	readOnly := bucket.tx.readOnly
	pathFragment, lastFragment := pi.fragment()
	for {
		if !readOnly {
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

		if lastFragment {
			break
		}
		pathFragment, lastFragment = pi.fragment()
	}

	// Create wrapper.
	childBucket := &Bucket{
		tx:   bucket.tx,
		name: pathFragment,
		b:    b,
	}

	// Done
	return childBucket, nil
}

// DeleteBucket removes an existing child bucket on the database
// NOTE: Inner sub-keys and buckets will be also deleted
func (bucket *Bucket) DeleteBucket(path []byte) error {
	// Check if TX is writable.
	if bucket.tx.readOnly {
		return ErrTxNotWritable
	}

	// Parse path.
	pi, err := newPathIterator(path)
	if err != nil {
		return err
	}

	// Go down until final fragment
	b := bucket.b
	pathFragment, lastFragment := pi.fragment()
	for !lastFragment {
		b = b.Bucket(pathFragment)
		if b == nil {
			return nil
		}

		pathFragment, lastFragment = pi.fragment()
	}

	// We are on the final fragment.
	err = b.DeleteBucket(pathFragment)

	// Done
	if err != nil && errors.Is(err, bolterrors.ErrBucketNotFound) {
		return nil // Ignore bucket not found errors.
	}
	return err
}

// Iterate creates an iterator object that allows to search for stored keys.
func (bucket *Bucket) Iterate() *Iterator {
	// Create wrapper.
	iter := Iterator{
		bucket: bucket,
		cursor: bucket.b.Cursor(),
	}

	// Done
	return &iter
}

func (bucket *Bucket) WithIterator(opts IteratorOptions, cb WithinIteratorCallback) error {
	if len(opts.Prefix) > 0 && len(opts.FirstKey) > 0 {
		return errors.New("prefix and first key cannot be used at the same time")
	}

	iter := bucket.Iterate()

	// Search for the first match.
	if len(opts.Prefix) > 0 {
		if !opts.Reverse {
			_ = iter.Seek(opts.Prefix, SeekPrefix)
		} else {
			_ = iter.Seek(opts.Prefix, SeekPrefixReverse)
		}
	} else if len(opts.FirstKey) > 0 {
		if !opts.Reverse {
			_ = iter.Seek(opts.FirstKey, SeekGreaterOrEqual)
		} else {
			_ = iter.Seek(opts.FirstKey, SeekLessOrEqual)
		}
	} else {
		if !opts.Reverse {
			_ = iter.First()
		} else {
			_ = iter.Last()
		}
	}

	// Iterate.
	for iter.IsValid() {
		// Call callback.
		stop, err := cb(iter)
		if err != nil {
			return err
		}
		if stop {
			break
		}

		// Advance to next item.
		if !opts.Reverse {
			_ = iter.Next()
		} else {
			_ = iter.Prev()
		}

		// If we passed a prefix as an option, check if it has it.
		if iter.IsValid() && len(opts.Prefix) > 0 {
			if !bytes.HasPrefix(iter.Key(), opts.Prefix) {
				break
			}
		}
	}

	// Done
	return nil
}

func (bucket *Bucket) Stats() BucketStats {
	return bucket.b.Stats()
}
