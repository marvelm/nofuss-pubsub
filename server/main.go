package main

import (
	"context"
	"log"
	"net"

	pb "github.com/marvelm/lk-pubsub/service"

	"github.com/dgraph-io/badger"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()

	conf := load_config()

	opts := badger.DefaultOptions(conf.data_dir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	offsets := gen_offsets(ctx, db)

	listener, err := net.Listen("tcp", conf.port)
	if err != nil {
		log.Fatalf("failed to Subscribe: %v", err)
	}
	defer listener.Close()

	s := grpc.NewServer()
	pb.RegisterLKPubsubServer(s, &server{config: conf, db: db, offsets: offsets})
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	defer s.Stop()
}
