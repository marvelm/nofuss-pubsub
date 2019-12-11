package main

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"time"

	pb "github.com/marvelm/lk-pubsub/service"

	"github.com/dgraph-io/badger"
	badger_pb "github.com/dgraph-io/badger/pb"
	"google.golang.org/grpc"
)

type server struct {
	db      *badger.DB
	offsets <-chan uint64
	pb.UnimplementedLKPubsubServer

	config
}

var key_prefix_internal = []byte("pubsub_")
var key_prefix_topic = []byte("pubsub_t")

const key_separator byte = 0x0

func fmt_key(topic, key []byte, offset uint64) []byte {
	// convert offset to bytes
	offset_b := make([]byte, 8)
	binary.BigEndian.PutUint64(offset_b, offset)

	// TODO pick a starting size to prevent allocations
	b := make([]byte, 0)

	b = append(b, key_prefix_topic...)
	b = append(b, key_separator)

	b = append(b, topic...)
	b = append(b, key_separator)

	b = append(b, offset_b...)
	b = append(b, key_separator)

	b = append(b, key...)
	return b
}

func fmt_topic_prefix(topic []byte) []byte {
	// TODO pick a starting size to prevent allocations
	b := make([]byte, 0)
	b = append(b, key_prefix_topic...)
	b = append(b, key_separator)

	b = append(b, topic...)
	b = append(b, key_separator)

	return b
}

func extract_from_key(fmted_key []byte, topic []byte) (key []byte, offset uint64) {
	topic_start_idx := len(key_prefix_topic)          // - 1 + len(key_separator)
	topic_end_idx := topic_start_idx + len(topic) + 1 // 1=key_separator
	offset_start_idx := topic_end_idx + 1
	offset_end_idx := offset_start_idx + 8

	offset_b := fmted_key[offset_start_idx:offset_end_idx]
	offset = binary.BigEndian.Uint64(offset_b)

	key = fmted_key[offset_end_idx+1:]
	return
}

func gen_offsets(ctx context.Context, db *badger.DB) <-chan uint64 {
	out := make(chan uint64)

	key := make([]byte, 0)
	key = append(key, key_prefix_internal...)
	key = append(key, key_separator)
	key = append(key, []byte("offsets")...)

	seq, err := db.GetSequence(key, 1000)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		defer seq.Release()
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				num, err := seq.Next()
				if err != nil {
					log.Fatal(err)
					return
				}
				out <- num
			}
		}
	}()

	return out
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutReply, error) {
	offset := <-s.offsets
	key := fmt_key(in.Topic, in.Key, offset)

	err := s.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, in.Data)
	})
	if err != nil {
		return nil, err
	}

	return &pb.PutReply{Offset: offset}, nil
}

func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.LKPubsub_SubscribeServer) error {
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// key-only iteration
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := fmt_topic_prefix(in.Topic)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			_, offset := extract_from_key(key, in.Topic)
			if offset < in.StartingOffset {
				continue
			}

			// get the value and send it iteration
			err := item.Value(func(b []byte) error {
				return stream.Send(&pb.SubscribeReply{
					Key:    key,
					Offset: offset,
				})
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return s.db.Subscribe(stream.Context(), func(kvs *badger_pb.KVList) {
		for _, kv := range kvs.Kv {
			key, offset := extract_from_key(kv.Key, in.Topic)
			if offset < in.StartingOffset {
				continue
			}

			err := stream.Send(&pb.SubscribeReply{
				Key:    key,
				Offset: offset,
			})
			if err != nil {
				log.Println(err)
				return
			}
		}
	}, fmt_topic_prefix(in.Topic))
}

type config struct {
	data_dir string

	retention time.Duration
	max_items uint64

	port string
}

func main() {
	ctx := context.Background()

	conf := config{
		data_dir:  "/tmp/lk-pubsub",
		retention: time.Hour,
		max_items: 100000,
		port:      ":50051",
	}

	opts := badger.DefaultOptions(conf.data_dir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

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
