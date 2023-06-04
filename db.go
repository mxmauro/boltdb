package boltdb

import (
	"go.etcd.io/bbolt"
)

// -----------------------------------------------------------------------------

// DB represents a database connection to BoltDB
type DB struct {
	db *bbolt.DB
}

// -----------------------------------------------------------------------------

// New returns a new database wrapper. If the database does not exist, it will be created.
func New(filename string) (*DB, error) {
	// Open creates or opens a database
	db, err := bbolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Create wrapper
	b := &DB{
		db: db,
	}

	// Done
	return b, nil
}

// Close closes the database connection
func (db *DB) Close() {
	_ = db.db.Close() // Well, if we cannot close a file, we won't be the only problem
}

// WithTx initiates a transaction and calls a callback
func (db *DB) WithTx(cb WithTxCallback, readOnly bool) error {
	wrappedCallback := func(tx *bbolt.Tx) error {
		cbTx := TX{
			db:       db,
			tx:       tx,
			readOnly: readOnly,
		}
		return cb(&cbTx)
	}

	if readOnly {
		return db.db.View(wrappedCallback)
	}
	return db.db.Update(wrappedCallback)
}
