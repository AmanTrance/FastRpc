package main

import (
	"context"
	"log"
	"net"

	pb "github.com/AmanTrance/FastRpc/benchmarks/grpc_bench/proto"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedEchoServiceServer
}

func (s *server) Echo(_ context.Context, in *pb.Payload) (*pb.Payload, error) {
	return in, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterEchoServiceServer(s, &server{})
	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
