package main

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	pb "github.com/marvelm/nofuss-pubsub/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestEnforceMaxItems(t *testing.T) {
	opts := badger.DefaultOptions("/tmp/test-enforce_max_items")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	defer db.DropAll()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := &server{db: db, offsets: gen_offsets(ctx, db), config: load_config()}

	s := grpc.NewServer()
	pb.RegisterNoFussPubSubServer(s, server)
	listener := bufconn.Listen(1024 * 1024)
	go s.Serve(listener)

	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewNoFussPubSubClient(conn)

	const max_items = 100
	const total = max_items + 20

	topic := []byte("bulk-insert")

	// Bulk generate some items with a topic
	for i := 0; i < total; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		client.Put(ctx, &pb.PutRequest{
			Topic: topic,
			Key:   b,
			Data:  []byte("blah"),
		})
	}

	// Enforce max retention
	trim_items(db, max_items)

	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Topic: topic})
	if err != nil {
		t.Error(err)
	}
	recv := make(chan *pb.SubscribeReply)
	go func() {
		for {
			item, err := stream.Recv()
			if err != nil {
				return
			}
			recv <- item
		}
	}()

	// count number of items
	num_records := 0
	timeout := time.After(time.Second)
recv_loop:
	for num_records < total {
		select {
		case <-timeout:
			break recv_loop
		case <-recv:
			num_records += 1
		}
	}

	if num_records != max_items {
		t.Error("num_records != max_items", num_records, max_items)
	}
}
