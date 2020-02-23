package main

import (
	"encoding/binary"
	"log"
	"testing"

	"github.com/dgraph-io/badger"
)

func TestEnforceMaxItems(t *testing.T) {
	opts := badger.DefaultOptions("/tmp/test-enforce_max_items")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	defer db.DropAll()

	const max_items = 100

	batch := db.NewWriteBatch()
	for i := 0; i < (max_items + 20); i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		if err := batch.Set(b, []byte("blah")); err != nil {
			t.Error(err)
		}
	}
	if err := batch.Flush(); err != nil {
		t.Error(err)
	}
	trim_items(db, max_items)

	num_records := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// key-only iteration
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().IsDeletedOrExpired() {
				log.Println("Deleted")
				continue
			}
			num_records += 1
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}

	if num_records != max_items {
		t.Error("num_records != max_items", num_records, max_items)
	}
}
