package main

import "time"

type config struct {
	data_dir string

	retention time.Duration
	max_items uint64

	port string
}

func load_config() config {
	return config{
		data_dir:  "/tmp/lk-pubsub",
		retention: time.Hour,
		max_items: 100000,
		port:      ":50051",
	}
}
