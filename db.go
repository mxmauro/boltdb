// See the LICENSE file for license details.

package boltdb

import (
	"errors"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// -----------------------------------------------------------------------------

// DB represents a database connection to a BoltDB database.
type DB struct {
	db       *bbolt.DB
	readOnly bool
}

// Options specifies a set of options when creating/opening the database.
type Options struct {
	ReadOnly    bool
	DirFileMode os.FileMode
	DbFileMode  os.FileMode
}

// -----------------------------------------------------------------------------

// New returns a new database wrapper. If the database does not exist, it will be created.
func New(filename string) (*DB, error) {
	return NewWithOptions(filename, Options{})
}

// NewWithOptions returns a new database wrapper using the provided options.
func NewWithOptions(filename string, opts Options) (*DB, error) {
	var fileMode os.FileMode

	// Create directory if writing to the database
	if !opts.ReadOnly {
		fileMode = 0700
		if opts.DirFileMode != 0 {
			fileMode = opts.DirFileMode
		}

		dir := filepath.Dir(filename)
		err := os.MkdirAll(dir, fileMode)
		if err != nil {
			return nil, err
		}
	}

	// Open/Create the database.
	fileMode = 0600
	if opts.DbFileMode != 0 {
		fileMode = opts.DbFileMode
	}
	db, err := bbolt.Open(filename, fileMode, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		ReadOnly:     opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}

	// Create wrapper.
	b := &DB{
		db:       db,
		readOnly: opts.ReadOnly,
	}

	// Done
	return b, nil
}

// Close closes the database connection.
func (db *DB) Close() {
	_ = db.db.Close() // Well, if we cannot close a file, we won't be the only problem.
}

// BeginTx starts a new transaction within the database.
func (db *DB) BeginTx(opts TxOptions) (*TX, error) {
	var err error

	// Validate options.
	if !opts.ReadOnly && db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// Create wrapper.
	tx := TX{
		db:       db,
		readOnly: opts.ReadOnly,
	}
	tx.tx, err = db.db.Begin(!opts.ReadOnly)
	if err != nil {
		return nil, err
	}

	// Done
	return &tx, nil
}

// WithinTx initiates a transaction and calls a callback.
func (db *DB) WithinTx(opts TxOptions, cb WithinTxCallback) error {
	tx, err := db.BeginTx(opts)
	if err == nil {
		err = cb(tx)
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}

	// Done
	return err
}

// Get returns the value of a key in the specified bucket or nil if not found.
func (db *DB) Get(bucket []byte, key []byte) ([]byte, error) {
	var value []byte

	err := db.WithinTx(TxOptions{ReadOnly: true}, func(tx *TX) error {
		b, err := tx.Bucket(bucket)
		if err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				return nil
			}
			return err
		}

		value = b.Get(key)
		return nil
	})

	// Done
	return value, err
}

// Put stores a key/value pair in the specified bucket.
func (db *DB) Put(bucket []byte, key []byte, value []byte) error {
	return db.WithinTx(TxOptions{}, func(tx *TX) error {
		b, err := tx.Bucket(bucket)
		if err != nil {
			return err
		}

		return b.Put(key, value)
	})
}

// Delete deletes a specific key in the specified bucket. No error is returned if key is not found.
func (db *DB) Delete(bucket []byte, key []byte) error {
	return db.WithinTx(TxOptions{}, func(tx *TX) error {
		b, err := tx.Bucket(bucket)
		if err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				return nil
			}
			return err
		}

		return b.Delete(key)
	})
}
