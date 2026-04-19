// See the LICENSE file for license details.

package boltdb_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/mxmauro/boltdb/v3"
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

	checkIteratorKey([]byte("k"), boltdb.SeekLessOrEqual, []byte("aaa"))
	checkIteratorKey(nil, boltdb.SeekLessOrEqual, []byte("zzz"))
	checkIteratorKey([]byte("d"), boltdb.SeekLessOrEqual, []byte("aaa"))
}

func TestIteratorCopyHelpers(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	bucketName := []byte("copy-helper-bucket")
	key := []byte("copy-helper-key")
	value := []byte("copy-helper-value")

	if err := db.Put(bucketName, key, value); err != nil {
		t.Fatalf("cannot write to test database [err=%v]", err.Error())
	}

	tx, err := db.BeginTx(boltdb.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("cannot begin transaction [err=%v]", err.Error())
	}
	defer tx.Rollback()

	bucket, err := tx.Bucket(bucketName)
	if err != nil {
		t.Fatalf("cannot get test bucket [err=%v]", err.Error())
	}

	iter := bucket.Iterate()
	if !iter.First() {
		t.Fatalf("cannot position iterator")
	}

	copiedKey := iter.CopyKey()
	copiedValue := iter.CopyValue()

	if !iter.Next() {
		iter.Last()
	}

	if !bytes.Equal(copiedKey, key) {
		t.Fatalf("unexpected copied key [got=%q]", copiedKey)
	}
	if !bytes.Equal(copiedValue, value) {
		t.Fatalf("unexpected copied value [got=%q]", copiedValue)
	}
}

func TestIteratorDeleteKeyAndBucket(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	err := db.WithinTx(boltdb.TxOptions{}, func(tx *boltdb.TX) error {
		bucket, err := tx.Bucket([]byte("iter-delete"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("alpha"), []byte("value-alpha")); err != nil {
			return err
		}

		childBucket, err := bucket.Bucket([]byte("child"))
		if err != nil {
			return err
		}
		return childBucket.Put([]byte("nested-key"), []byte("nested-value"))
	})
	if err != nil {
		t.Fatalf("cannot prepare test data [err=%v]", err.Error())
	}

	err = db.WithinTx(boltdb.TxOptions{}, func(tx *boltdb.TX) error {
		bucket, err := tx.Bucket([]byte("iter-delete"))
		if err != nil {
			return err
		}

		iter := bucket.Iterate()
		if !iter.Seek([]byte("alpha"), boltdb.SeekExact) {
			t.Fatalf("cannot position iterator on key entry")
		}
		if err := iter.Delete(); err != nil {
			return err
		}

		if !iter.Seek([]byte("child"), boltdb.SeekExact) {
			t.Fatalf("cannot position iterator on nested bucket entry")
		}
		return iter.Delete()
	})
	if err != nil {
		t.Fatalf("cannot delete through iterator [err=%v]", err.Error())
	}

	err = db.WithinTx(boltdb.TxOptions{ReadOnly: true}, func(tx *boltdb.TX) error {
		bucket, err := tx.Bucket([]byte("iter-delete"))
		if err != nil {
			return err
		}
		if value := bucket.Get([]byte("alpha")); value != nil {
			t.Fatalf("expected key to be deleted [got=%q]", value)
		}
		_, err = bucket.Bucket([]byte("child"))
		if !errors.Is(err, boltdb.ErrBucketNotFound) {
			t.Fatalf("expected nested bucket to be deleted [got=%v]", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cannot verify iterator deletions [err=%v]", err.Error())
	}
}

func TestIteratorDeleteInvalidPosition(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	err := db.WithinTx(boltdb.TxOptions{}, func(tx *boltdb.TX) error {
		bucket, err := tx.Bucket([]byte("iter-invalid-delete"))
		if err != nil {
			return err
		}
		iter := bucket.Iterate()
		if err := iter.Delete(); !errors.Is(err, boltdb.ErrInvalidCursorPosition) {
			t.Fatalf("expected ErrInvalidCursorPosition [got=%v]", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected transaction error [err=%v]", err.Error())
	}
}

func TestIteratorReverseEdgeCases(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	bucketName := []byte("iter-reverse-edge")
	keys := [][]byte{
		[]byte("prefix"),
		[]byte("prefix\xff"),
		[]byte("prefix\xff\x01"),
		[]byte("prefix\xff\xff"),
		[]byte("prefix\xff\xff\x01"),
		[]byte("zzz"),
	}

	for _, key := range keys {
		if err := db.Put(bucketName, key, []byte("value")); err != nil {
			t.Fatalf("cannot write test key %q [err=%v]", key, err.Error())
		}
	}

	tx, err := db.BeginTx(boltdb.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("cannot begin transaction [err=%v]", err.Error())
	}
	defer tx.Rollback()

	bucket, err := tx.Bucket(bucketName)
	if err != nil {
		t.Fatalf("cannot get test bucket [err=%v]", err.Error())
	}

	iter := bucket.Iterate()

	checkSeek := func(key []byte, method boltdb.SeekMethod, expectedKey []byte) {
		t.Helper()

		if ok := iter.Seek(key, method); !ok {
			if expectedKey == nil {
				return
			}
			t.Fatalf("seek failed unexpectedly for key %q with method=%s", key, seekMethod2string(method))
		}
		if expectedKey == nil {
			t.Fatalf("expected seek miss for key %q with method=%s [got=%q]", key, seekMethod2string(method), iter.Key())
		}
		if !bytes.Equal(iter.Key(), expectedKey) {
			t.Fatalf("unexpected seek result for key %q with method=%s [got=%q want=%q]", key, seekMethod2string(method), iter.Key(), expectedKey)
		}
	}

	checkSeek([]byte("prefix\xff"), boltdb.SeekPrefixReverse, []byte("prefix\xff\xff\x01"))
	checkSeek([]byte("prefix\xff\xff"), boltdb.SeekPrefixReverse, []byte("prefix\xff\xff\x01"))
	checkSeek([]byte("prefix\xff\xff\xff"), boltdb.SeekPrefixReverse, nil)
	checkSeek([]byte("prefix\xff\xff"), boltdb.SeekLessOrEqual, []byte("prefix\xff\xff"))
	checkSeek([]byte("prefix\xff\xff\x00"), boltdb.SeekLessOrEqual, []byte("prefix\xff\xff"))
	checkSeek([]byte("aaa"), boltdb.SeekLessOrEqual, nil)
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
