package boltdb

import (
	"bytes"

	"go.etcd.io/bbolt"
)

// -----------------------------------------------------------------------------

// SeekMethod specifies the search method for the iterator.
type SeekMethod int

const (
	SeekExact          SeekMethod = iota
	SeekPrefix         SeekMethod = iota
	SeekPrefixReverse  SeekMethod = iota
	SeekGreaterOrEqual SeekMethod = iota
	SeekLessOrEqual    SeekMethod = iota
)

// Iterator encapsulates a bucket key/value iterator
type Iterator struct {
	bucket *Bucket
	cursor *bbolt.Cursor
	key    []byte
	value  []byte
}

// WithIteratorOptions specifies a set of options when creating a new iterator.
// NOTE: Prefix and FirstKey cannot be used at the same time.
type WithIteratorOptions struct {
	// Reverse scan keys in reverse order.
	Reverse bool

	// Prefix filters the iterator to keys with the given prefix. It cannot be used in conjunction with FirstKey.
	Prefix []byte

	// FirstKey sets the start point of the iterator. It cannot be used with Prefix.
	FirstKey []byte
}

// WithinIteratorCallback is a callback called for every key found in the given request
// NOTE: If value == nil, then they key points to a child bucket
type WithinIteratorCallback func(iter *Iterator) (stop bool, err error)

// -----------------------------------------------------------------------------

// IsValid returns true if the iterator is pointing to some value or nested bucket.
func (iter *Iterator) IsValid() bool {
	return iter.key != nil
}

// Key gets the current iterator key. Nil if reached the end or a search failed. The key is valid until
// the iterator position is changed.
func (iter *Iterator) Key() []byte {
	return iter.key
}

// CopyKey acts like Key but returns a copy of the key, so it remains valid after moving the iterator
// position.
func (iter *Iterator) CopyKey() []byte {
	return cloneBytes(iter.key)
}

// HasKeyPrefix checks if the current key has the provided prefix.
func (iter *Iterator) HasKeyPrefix(prefix []byte) bool {
	if len(iter.key) == 0 {
		return len(prefix) == 0
	}
	return bytes.HasPrefix(iter.key, prefix)
}

// Value gets the current iterator value. The value is valid until the iterator position is changed.
// IMPORTANT: If value is nil, then the key points to a nested bucket name.
func (iter *Iterator) Value() []byte {
	return iter.value
}

// CopyValue acts like Value but returns a copy of the value, so it remains valid after moving the iterator
// position.
func (iter *Iterator) CopyValue() []byte {
	if iter.value == nil {
		return nil
	}
	copiedValue := make([]byte, len(iter.value))
	copy(copiedValue, iter.value)
	return copiedValue
}

// IsNestedBucket returns true if the iterator is pointing to a nested bucket.
func (iter *Iterator) IsNestedBucket() bool {
	return iter.key != nil && iter.value == nil
}

// Bucket returns the bucket associated with this iterator.
func (iter *Iterator) Bucket() *Bucket {
	return iter.bucket
}

// First moves the iterator to the first entry inside the bucket.
func (iter *Iterator) First() bool {
	iter.key, iter.value = iter.cursor.First()
	return iter.key != nil
}

// Last moves the iterator to the last entry inside the bucket.
func (iter *Iterator) Last() bool {
	iter.key, iter.value = iter.cursor.Last()
	return iter.key != nil
}

// Next moves the iterator to the next entry inside the bucket.
func (iter *Iterator) Next() bool {
	iter.key, iter.value = iter.cursor.Next()
	return iter.key != nil
}

// Prev moves the iterator to the previous entry inside the bucket.
func (iter *Iterator) Prev() bool {
	iter.key, iter.value = iter.cursor.Prev()
	return iter.key != nil
}

// Seek searches for a key match with the provided prefix and method. Prefix can be nil.
func (iter *Iterator) Seek(prefix []byte, method SeekMethod) bool {
	origPrefix := prefix

	if len(prefix) > 0 && method == SeekPrefixReverse {
		var i int

		prefix = make([]byte, len(origPrefix))
		copy(prefix, origPrefix)
		for i = len(prefix) - 1; i >= 0; i-- {
			prefix[i] = prefix[i] + 1
			if prefix[i] != 0 {
				break
			}
		}
		prefix = prefix[:i+1]
	}

	// If no prefix was given.
	if len(prefix) == 0 {
		switch method {
		case SeekPrefix:
			fallthrough
		case SeekGreaterOrEqual:
			return iter.First()

		case SeekPrefixReverse:
			fallthrough
		case SeekLessOrEqual:
			return iter.Last()

		case SeekExact:
		}

		return iter.clean()
	}

	// Search for the prefix.
	iter.key, iter.value = iter.cursor.Seek(prefix)

	switch method {
	case SeekExact:
		if len(iter.key) == 0 || !bytes.Equal(iter.key, prefix) {
			return iter.clean()
		}

	case SeekPrefix:
		if len(iter.key) == 0 || !bytes.HasPrefix(iter.key, prefix) {
			return iter.clean()
		}

	case SeekPrefixReverse:
		if len(iter.key) == 0 {
			if !iter.Last() {
				return iter.clean()
			}
		} else {
			if !iter.Prev() {
				return iter.clean()
			}
		}
		if !bytes.HasPrefix(iter.key, origPrefix) {
			return iter.clean()
		}

	case SeekGreaterOrEqual:
		// Seek to already search for the next item.
		if len(iter.key) == 0 {
			return iter.clean()
		}

	case SeekLessOrEqual:
		if len(iter.key) == 0 {
			if !iter.Last() {
				return iter.clean()
			}
		} else if bytes.Compare(iter.key, origPrefix) > 0 {
			if !iter.Prev() {
				return iter.clean()
			}
		}
	}

	// Done
	return true
}

// Delete deletes the current key the iterator is pointing to.
func (iter *Iterator) Delete() error {
	if iter.key == nil {
		return ErrInvalidCursorPosition
	}
	if iter.value != nil {
		return iter.cursor.Delete()
	}
	return iter.bucket.DeleteBucket(iter.key)
}

func (iter *Iterator) clean() bool {
	iter.key, iter.value = nil, nil
	return false
}
