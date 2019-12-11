package main

import (
	"context"
	"log"
	"time"

	pb "github.com/marvelm/lk-pubsub/service"

	"google.golang.org/grpc"
)

const (
	address = "localhost:50051"
)

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewLKPubsubClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go subscribe(ctx, c)
	time.Sleep(time.Second)

	r, err := c.Put(ctx, &pb.PutRequest{Topic: []byte("My topic"), Key: []byte("Some key")})
	if err != nil {
		log.Fatalf("could not put: %v", err)
	}
	log.Printf("Offset: %d", r.Offset)
}

func subscribe(ctx context.Context, c pb.LKPubsubClient) {
	stream, err := c.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:          []byte("My topic"),
		StartingOffset: 4000,
	})

	if err != nil {
		log.Fatal(err)
	}

	for {
		reply, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}
		log.Println(reply.Offset)
	}
}
