package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "hello/hello"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	poolSize := 100
	connectionPool := make(chan pb.GreeterClient, poolSize)

	for range poolSize {
		conn, err := grpc.NewClient("10.25.135.134:10000", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}

		c := pb.NewGreeterClient(conn)
		connectionPool <- c
	}

	numCalls := 1000
	size := 1 * 1024 * 1024

	payload := make([]byte, size)
	for range size {
		payload = append(payload, 'A')
	}

	var wg sync.WaitGroup
	wg.Add(numCalls)

	startTime := time.Now()

	for range numCalls {
		go func() {
			defer wg.Done()

			c := <-connectionPool
			defer func() {
				connectionPool <- c
			}()

			_, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: string(payload)})
			if err != nil {
				log.Fatalf("could not greet: %v", err)
			}
		}()
	}

	wg.Wait()

	duration := time.Since(startTime)

	log.Printf("--- TestNetwork_LargeDataStress Complete ---")
	log.Printf("Total time for %d calls: %v", numCalls, duration)
	log.Printf("Avg. time per call: %v", duration/time.Duration(numCalls))
	log.Printf("Avg. calls/sec: %.2f\n", float64(numCalls)/duration.Seconds())
}
