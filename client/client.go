package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	pb "github.com/marvelm/nofuss-pubsub/service"
)

type Config struct {
	address string
}

func NewClient(ctx context.Context, config Config) (*pb.NoFussPubSubClient, error) {
	conn, err := grpc.Dial(config.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	}
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	cl := pb.NewNoFussPubSubClient(conn)
	return &cl, nil
}
