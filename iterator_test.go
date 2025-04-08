// See the LICENSE file for license details.

package boltdb_test

import (
	"bytes"
	"testing"

	"github.com/mxmauro/boltdb/v2"
)

// -----------------------------------------------------------------------------

func TestIterator(t *testing.T) {
	var tx *boltdb.TX
	var bucket *boltdb.Bucket

	db := openTestDb(t)
	defer db.Close()

	bucketName := []byte("test-bucket")
	dummyValue := []byte("dummy-value")

	err := db.Put(bucketName, []byte("aaa"), dummyValue)
	if err == nil {
		err = db.Put(bucketName, []byte("key1"), dummyValue)
	}
	if err == nil {
		err = db.Put(bucketName, []byte("key1-a"), dummyValue)
	}
	if err == nil {
		err = db.Put(bucketName, []byte("key1-b"), dummyValue)
	}
	if err == nil {
		err = db.Put(bucketName, []byte("key2"), dummyValue)
	}
	if err == nil {
		err = db.Put(bucketName, []byte("key3"), dummyValue)
	}
	if err == nil {
		err = db.Put(bucketName, []byte("zzz"), dummyValue)
	}
	if err != nil {
		t.Fatalf("cannot write to test database [err=%v]", err.Error())
	}

	tx, err = db.BeginTx(boltdb.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("cannot begin transaction [err=%v]", err.Error())
	}
	defer tx.Rollback()

	bucket, err = tx.Bucket(bucketName)
	if err != nil {
		t.Fatalf("cannot get test bucket [err=%v]", err.Error())
	}

	iter := bucket.Iterate()

	checkIteratorKey := func(key []byte, method boltdb.SeekMethod, expectedKey []byte) {
		match := false

		_ = iter.Seek(key, method)
		if expectedKey != nil {
			if bytes.Equal(iter.Key(), expectedKey) {
				match = true
			}
		} else if iter.Key() == nil {
			match = true
		}
		if !match {
			t.Fatalf("seek for key '%v' does not match the expected one '%v' using method=%v [got %v]",
				string(key), string(expectedKey), seekMethod2string(method), string(iter.Key()))
		}
	}

	checkIteratorKey([]byte("key1"), boltdb.SeekExact, []byte("key1"))
	checkIteratorKey([]byte("key3"), boltdb.SeekExact, []byte("key3"))
	checkIteratorKey([]byte("zzz"), boltdb.SeekExact, []byte("zzz"))

	checkIteratorKey([]byte("k"), boltdb.SeekPrefix, []byte("key1"))
	checkIteratorKey([]byte("key1-"), boltdb.SeekPrefix, []byte("key1-a"))

	checkIteratorKey([]byte("k"), boltdb.SeekPrefixReverse, []byte("key3"))
	checkIteratorKey([]byte("key1-"), boltdb.SeekPrefixReverse, []byte("key1-b"))

	checkIteratorKey([]byte("k"), boltdb.SeekGreaterOrEqual, []byte("key1"))
	checkIteratorKey(nil, boltdb.SeekGreaterOrEqual, []byte("aaa"))
	checkIteratorKey([]byte("t"), boltdb.SeekGreaterOrEqual, []byte("zzz"))

	checkIteratorKey([]byte("k"), boltdb.SeekLessOrEqual, []byte("key3"))
	checkIteratorKey(nil, boltdb.SeekLessOrEqual, []byte("zzz"))
	checkIteratorKey([]byte("d"), boltdb.SeekLessOrEqual, []byte("aaa"))
}

func seekMethod2string(m boltdb.SeekMethod) string {
	switch m {
	case boltdb.SeekExact:
		return "exact"
	case boltdb.SeekPrefix:
		return "prefix"
	case boltdb.SeekPrefixReverse:
		return "prefix-reverse"
	case boltdb.SeekGreaterOrEqual:
		return "greater-or-equal"
	case boltdb.SeekLessOrEqual:
		return "less-or-equal"
	}
	return ""
}
