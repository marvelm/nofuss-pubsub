package main

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/dgraph-io/badger"
)

func retention_thread(ctx context.Context, db *badger.DB, max_items uint64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			trim_items(db, max_items)
		}
	}
}

func trim_items(db *badger.DB, max_items uint64) {
	iteration_max := max_items * 2

	// Count the total number of items
	var num_items uint64 = 0
	db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// key-only iteration
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().IsDeletedOrExpired() {
				continue
			}

			// Pick only keys that are part of a topic
			if !bytes.HasPrefix(it.Item().Key(), key_prefix_topic) {
				continue
			}

			// Prevents this function from neverending as
			// items keep piling up
			if num_items > iteration_max {
				break
			}

			num_items += 1
		}

		return nil
	})

	to_delete := num_items - max_items
	// nothing to delete
	if to_delete < 0 {
		return
	}

	// Trim until to_delete
	err := db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// key-only iteration
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var i uint64 = 0
		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().IsDeletedOrExpired() {
				continue
			}

			// Only keys that are part of a topic
			if !bytes.HasPrefix(it.Item().Key(), key_prefix_topic) {
				continue
			}

			i += 1
			if i > to_delete {
				break
			}

			// Copy here because it.Item.Key is mutating in-place
			err := txn.Delete(it.Item().KeyCopy(nil))
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Println("Failed to trim items", err)
	}
}
