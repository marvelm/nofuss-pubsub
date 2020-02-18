package main

import (
	"context"
	"encoding/binary"
	"log"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

func TestEnforceMaxItems(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	opts := badger.DefaultOptions("/tmp/test-enforce_max_items")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	defer db.DropAll()

	const max_items = 100
	enforce_max_items(ctx, db, max_items)

	for i := 0; i < max_items+20; i++ {
		err := db.Update(func(tx *badger.Txn) error {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(i))
			entry := badger.NewEntry(b, []byte("blah"))
			return tx.SetEntry(entry)
		})
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(time.Second * 10)

	num_records := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// key-only iteration
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
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
