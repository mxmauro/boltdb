// See the LICENSE file for license details.

package boltdb_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mxmauro/boltdb/v2"
)

// -----------------------------------------------------------------------------

func TestNestedBuckets(t *testing.T) {
	db := openTestDb(t)
	defer db.Close()

	err := db.WithinTx(boltdb.TxOptions{}, func(tx *boltdb.TX) error {
		for pIdx := 1; pIdx <= 4; pIdx++ {
			for chIdx := 1; chIdx <= 2; chIdx++ {
				for subChIdx := 1; subChIdx <= 5; subChIdx++ {
					b, err := tx.Bucket([]byte(fmt.Sprintf("/parent%v//child%v////subchild%v///", pIdx, chIdx, subChIdx)))
					if err != nil {
						return fmt.Errorf("cannot create buckets [err=%v]", err.Error())
					}

					err = b.Put([]byte("dummy-key"), []byte("dummy-value"))
					if err != nil {
						return fmt.Errorf("cannot write to test database [err=%v]", err.Error())
					}
				}

				b, err := tx.Bucket([]byte(fmt.Sprintf("parent%v/child%v", pIdx, chIdx)))
				if b == nil {
					return fmt.Errorf("cannot locate buckets [err=%v]", err.Error())
				}

				for subChIdx := 6; subChIdx <= 8; subChIdx++ {
					var b2 *boltdb.Bucket

					b2, err = b.Bucket([]byte(fmt.Sprintf("/subchild%v///", subChIdx)))
					if err != nil {
						return fmt.Errorf("cannot create buckets [err=%v]", err.Error())
					}

					err = b2.Put([]byte("dummy-key"), []byte("dummy-value"))
					if err != nil {
						return fmt.Errorf("cannot write to test database [err=%v]", err.Error())
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = db.WithinTx(boltdb.TxOptions{ReadOnly: true}, func(tx *boltdb.TX) error {
		for pIdx := 1; pIdx <= 4; pIdx++ {
			for chIdx := 1; chIdx <= 2; chIdx++ {
				for subChIdx := 1; subChIdx <= 8; subChIdx++ {
					b, _ := tx.Bucket([]byte(fmt.Sprintf("/parent%v//child%v////subchild%v///", pIdx, chIdx, subChIdx)))
					if b == nil {
						return fmt.Errorf("cannot locate bucket [err=%v]", err.Error())
					}

					value := b.Get([]byte("dummy-key"))
					if value == nil {
						return errors.New("cannot find key/value pair")
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
}
