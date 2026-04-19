// See the LICENSE file for license details.

package boltdb_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mxmauro/boltdb/v3"
)

// -----------------------------------------------------------------------------

func TestSimpleAccess(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	for bucketId := 1; bucketId <= 2; bucketId++ {
		for keyId := 1; keyId <= 2; keyId++ {
			err := db.Put([]byte(fmt.Sprintf("bucket-%d", bucketId)), []byte(fmt.Sprintf("key-%d", keyId)), []byte(fmt.Sprintf("value-%d", keyId)))
			if err != nil {
				t.Fatalf("cannot write to test database [err=%v]", err.Error())
			}
		}
	}

	for bucketId := 1; bucketId <= 2; bucketId++ {
		for keyId := 1; keyId <= 2; keyId++ {
			value, err := db.Get([]byte(fmt.Sprintf("bucket-%d", bucketId)), []byte(fmt.Sprintf("key-%d", keyId)))
			if err != nil {
				t.Fatalf("cannot read from test database [err=%v]", err.Error())
			}
			if value == nil || bytes.Compare(value, []byte(fmt.Sprintf("value-%d", keyId))) != 0 {
				t.Fatalf("wrong value read from test database")
			}
		}
	}
}

func TestTransaction(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	err := db.WithinTx(boltdb.TxOptions{}, func(tx *boltdb.TX) error {
		for bucketId := 11; bucketId <= 12; bucketId++ {
			b, err2 := tx.Bucket([]byte(fmt.Sprintf("bucket-%d", bucketId)))
			if err2 != nil {
				return err2
			}
			for keyId := 11; keyId <= 12; keyId++ {
				err2 = b.Put([]byte(fmt.Sprintf("key-%d", keyId)), []byte(fmt.Sprintf("value-%d", keyId)))
				if err2 != nil {
					return err2
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cannot write to test database [err=%v]", err.Error())
	}

	err = db.WithinTx(boltdb.TxOptions{ReadOnly: true}, func(tx *boltdb.TX) error {
		for bucketId := 11; bucketId <= 12; bucketId++ {
			b, err2 := tx.Bucket([]byte(fmt.Sprintf("bucket-%d", bucketId)))
			if err2 != nil {
				return err2
			}
			for keyId := 11; keyId <= 12; keyId++ {
				value := b.Get([]byte(fmt.Sprintf("key-%d", keyId)))
				if value == nil || bytes.Compare(value, []byte(fmt.Sprintf("value-%d", keyId))) != 0 {
					return errors.New("wrong value read")
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cannot read from test database [err=%v]", err.Error())
	}
}

func TestGetReturnsStableCopy(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	bucketName := []byte("stable-copy-bucket")
	key := []byte("stable-copy-key")
	initialValue := []byte("value-1")

	if err := db.Put(bucketName, key, initialValue); err != nil {
		t.Fatalf("cannot write initial value [err=%v]", err.Error())
	}

	value, err := db.Get(bucketName, key)
	if err != nil {
		t.Fatalf("cannot read initial value [err=%v]", err.Error())
	}
	if !bytes.Equal(value, initialValue) {
		t.Fatalf("wrong initial value [got=%q]", value)
	}

	if err := db.Put(bucketName, key, []byte("value-2")); err != nil {
		t.Fatalf("cannot overwrite value [err=%v]", err.Error())
	}

	if !bytes.Equal(value, initialValue) {
		t.Fatalf("returned value was not stable after transaction end [got=%q]", value)
	}
}

func TestReadOnlyDatabaseRejectsWrites(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	bucketName := []byte("read-only-bucket")
	key := []byte("read-only-key")
	value := []byte("read-only-value")

	if err := db.Put(bucketName, key, value); err != nil {
		t.Fatalf("cannot seed test database [err=%v]", err.Error())
	}
	db.Close()

	readOnlyDb, err := boltdb.NewWithOptions(filepath.Join(t.TempDir(), "missing.db"), boltdb.Options{ReadOnly: true})
	if err == nil {
		readOnlyDb.Close()
		t.Fatalf("expected opening a missing read-only database to fail")
	}

	filename := filepath.Join(t.TempDir(), "read-only.db")
	writableDb, err := boltdb.New(filename)
	if err != nil {
		t.Fatalf("cannot create writable test database [err=%v]", err.Error())
	}
	if err := writableDb.Put(bucketName, key, value); err != nil {
		writableDb.Close()
		t.Fatalf("cannot seed writable database [err=%v]", err.Error())
	}
	writableDb.Close()

	readOnlyDb, err = boltdb.NewWithOptions(filename, boltdb.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("cannot reopen read-only database [err=%v]", err.Error())
	}
	defer readOnlyDb.Close()

	if err := readOnlyDb.Put(bucketName, key, []byte("new-value")); !errors.Is(err, boltdb.ErrDatabaseReadOnly) {
		t.Fatalf("expected ErrDatabaseReadOnly [got=%v]", err)
	}
}

func openTestDb(t *testing.T) *boltdb.DB {
	var db *boltdb.DB
	var err error

	filename := filepath.Join(t.TempDir(), "test.db")

	db, err = boltdb.New(filename)
	if err != nil {
		t.Fatalf("cannot create test database [err=%v]", err.Error())
	}

	return db
}
