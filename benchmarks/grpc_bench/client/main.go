package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AmanTrance/FastRpc/benchmarks/common"
	pb "github.com/AmanTrance/FastRpc/benchmarks/grpc_bench/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const numCalls = 1000

func main() {
	payloadData := common.GeneratePayload()

	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewEchoServiceClient(conn)

	log.Printf("Dispatching %d concurrent calls with %d MB payload...\n", numCalls, common.PayloadSize/(1024*1024))

	var wg sync.WaitGroup
	wg.Add(numCalls)

	startTime := time.Now()

	for i := 0; i < numCalls; i++ {
		go func(callIndex int) {
			defer wg.Done()

			req := &pb.Payload{Data: payloadData}

			resp, err := client.Echo(context.Background(), req)
			if err != nil {
				log.Printf("Call %d failed: %v", callIndex, err)
				return
			}

			if len(resp.Data) != len(payloadData) {
				log.Printf("Call %d: Data size mismatch!", callIndex)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Println("\n--- gRPC Benchmark Complete ---")
	fmt.Printf("Total time: %v\n", duration)
	fmt.Printf("Total calls: %d\n", numCalls)
	fmt.Printf("Avg. time per call: %v\n", duration/time.Duration(numCalls))
	fmt.Printf("Avg. calls/sec: %.2f\n", float64(numCalls)/duration.Seconds())
}
