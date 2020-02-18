package main

import (
	"context"
	"log"
	"time"

	"github.com/dgraph-io/badger"
)

func enforce_max_items(ctx context.Context, db *badger.DB, max_items uint64) {
	go func() {
		tick := time.Tick(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick:
				var current_num_items uint64 = 0
				for _, table := range db.Tables(true) {
					current_num_items += table.KeyCount
				}

				var trim_until uint64 = 0
				err := db.View(func(tx *badger.Txn) error {
					opts := badger.DefaultIteratorOptions
					// key-only iteration
					opts.PrefetchValues = false
					it := tx.NewIterator(opts)
					defer it.Close()

					var current_num_items uint64 = 0
					for it.Rewind(); it.Valid(); it.Next() {
						current_num_items += 1
					}

					if current_num_items > max_items {
						trim_until = current_num_items - max_items
					}
					return nil
				})
				if err != nil {
					log.Println(err)
					continue
				}

				err = db.Update(delete_until_index(trim_until))
				if err != nil {
					log.Println("Failed to trim items", err)
				}
			}
		}
	}()
}

func delete_until_index(end uint64) func(txn *badger.Txn) error {
	return func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// key-only iteration
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var i uint64 = 0
		for it.Rewind(); it.Valid(); it.Next() {
			if i > end {
				return nil
			}
			i += 1

			item := it.Item()
			key := item.Key()
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}
}
