package boltdb

import (
	"bytes"
	"errors"

	"go.etcd.io/bbolt"
)

// -----------------------------------------------------------------------------

// Bucket is an implementation of Bucket within a transaction in BoltDB
type Bucket struct {
	tx   *TX
	name []byte
	b    *bbolt.Bucket
}

type BucketStats = bbolt.BucketStats

// ForEachCallback is a callback that is called for every key found in the given request
// NOTE: If value == nil, then they key points to a child bucket
type ForEachCallback func(bucket *Bucket, key []byte, value []byte) (stop bool, err error)

// -----------------------------------------------------------------------------

// DB gets the database this bucket belongs to
func (bucket *Bucket) DB() *DB {
	return bucket.tx.db
}

// TX gets the transaction this bucket belongs to
func (bucket *Bucket) TX() *TX {
	return bucket.tx
}

// Name returns the bucket name
func (bucket *Bucket) Name() []byte {
	return bucket.name
}

// NextSequence returns an autoincrement integer for the bucket
func (bucket *Bucket) NextSequence() (uint64, error) {
	return bucket.b.NextSequence()
}

// Get returns the value of a key in a bucket or nil if not found
func (bucket *Bucket) Get(key []byte) []byte {
	return bucket.b.Get(key)
}

// Put stores a key/value pair in the bucket
func (bucket *Bucket) Put(key []byte, value []byte) error {
	return bucket.b.Put(key, value)
}

// Delete deletes a specific key
func (bucket *Bucket) Delete(key []byte) error {
	return bucket.b.Delete(key)
}

// DeleteWithPrefix deletes a set of keys
func (bucket *Bucket) DeleteWithPrefix(keyPrefix []byte) error {
	return bucket.ForEachWithKeyPrefix(keyPrefix, func(bucket *Bucket, key []byte, v []byte) (bool, error) {
		if v == nil {
			return true, nil // Ignore child buckets
		}
		return true, bucket.b.Delete(key)
	})
}

// ForEach calls a callback for all the keys within the bucket
func (bucket *Bucket) ForEach(cb ForEachCallback) error {
	c := bucket.b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		stop, err := cb(bucket, k, v)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// ForEachWithKeyPrefix calls a callback for all the keys starting with the provided prefix within the bucket
func (bucket *Bucket) ForEachWithKeyPrefix(keyPrefix []byte, cb ForEachCallback) error {
	c := bucket.b.Cursor()
	for k, v := c.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = c.Next() {
		stop, err := cb(bucket, k, v)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// Bucket returns a bucket on the database (and creates if it does not exist)
func (bucket *Bucket) Bucket(path []byte) (*Bucket, error) {
	var b *bbolt.Bucket

	path = removeLeadingSlashes(path)
	nameLen := getPathFragmentLen(path)
	if nameLen < 1 {
		return nil, errors.New("invalid path")
	}

	if !bucket.tx.readOnly {
		var err error

		b, err = bucket.b.CreateBucketIfNotExists(path[0:nameLen])
		if err != nil {
			return nil, err
		}
	} else {
		b = bucket.b.Bucket(path[0:nameLen])
		if b == nil {
			return nil, bbolt.ErrBucketNotFound
		}
	}

	// Initialize bucket object
	childBucket := &Bucket{
		tx:   bucket.tx,
		name: path[0:nameLen],
		b:    b,
	}

	// Get child bucket if requested
	path = removeLeadingSlashes(path[nameLen:])
	if len(path) > 0 {
		var err error

		childBucket, err = childBucket.Bucket(path)
		if err != nil {
			return nil, err
		}
	}

	// Done
	return childBucket, nil
}

// DeleteBucket removes an existing child bucket on the database
// NOTE: Inner subkeys and buckets will be also deleted
func (bucket *Bucket) DeleteBucket(path []byte) error {
	var err error

	if bucket.tx.readOnly {
		return bbolt.ErrTxNotWritable
	}

	path = removeLeadingSlashes(removeTrailingSlashes(path))
	lastSlash := bytes.LastIndexByte(path, '/')
	if lastSlash < 0 {
		err = bucket.b.DeleteBucket(path)
	} else {
		var childBucket *Bucket

		childBucket, err = bucket.Bucket(path[0:lastSlash])
		if err == nil {
			err = childBucket.DeleteBucket(path[lastSlash+1:])
		}
	}

	// Ignore bucket not found errors
	if err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
		return err
	}
	return nil
}

func (bucket *Bucket) Stats() BucketStats {
	return bucket.b.Stats()
}
