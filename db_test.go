package boltdb_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mxmauro/boltdb"
)

// -----------------------------------------------------------------------------

func TestSimpleAccess(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	for bucketId := 1; bucketId <= 2; bucketId++ {
		for keyId := 1; keyId <= 2; keyId++ {
			err := db.Put([]byte(fmt.Sprintf("bucket-%d", bucketId)), []byte(fmt.Sprintf("key-%d", keyId)), []byte(fmt.Sprintf("value-%d", keyId)))
			if err != nil {
				t.Fatalf("cannot write to test database [zone=1] [err=%v]", err.Error())
			}
		}
	}

	for bucketId := 1; bucketId <= 2; bucketId++ {
		for keyId := 1; keyId <= 2; keyId++ {
			value, err := db.Get([]byte(fmt.Sprintf("bucket-%d", bucketId)), []byte(fmt.Sprintf("key-%d", keyId)))
			if err != nil {
				t.Fatalf("cannot read from test database [zone=1] [err=%v]", err.Error())
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

	err := db.WithTx(func(tx *boltdb.TX) error {
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
	}, false)
	if err != nil {
		t.Fatalf("cannot write to test database [err=%v]", err.Error())
	}

	err = db.WithTx(func(tx *boltdb.TX) error {
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
	}, true)
	if err != nil {
		t.Fatalf("cannot read from test database [err=%v]", err.Error())
	}
}

func openTestDb(t *testing.T) *boltdb.DB {
	var db *boltdb.DB

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("cannot get current directory [err=%v]", err.Error())
	}
	filename := filepath.Join(dir, "testdata", "test.db")

	db, err = boltdb.New(filename)
	if err != nil {
		t.Fatalf("cannot create test database [err=%v]", err.Error())
	}

	return db
}
