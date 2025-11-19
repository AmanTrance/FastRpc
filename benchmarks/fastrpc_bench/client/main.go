package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	fastrpc "github.com/AmanTrance/FastRpc"
	"github.com/AmanTrance/FastRpc/benchmarks/common"
)

const numCalls = 1000

func main() {
	ip := net.IPv4(127, 0, 0, 1)
	port := 10000
	poolSize := 20

	payloadData := common.GeneratePayload()

	log.Printf("Connecting to master at %s:%d with pool size %d\n", ip, port, poolSize)
	slave, err := fastrpc.NewSlave(ip, port, poolSize)
	if err != nil {
		log.Fatalf("Failed to create slave: %v", err)
	}
	defer slave.DeInitialize()

	var wg sync.WaitGroup
	wg.Add(numCalls)

	log.Printf("Dispatching %d concurrent calls...\n", numCalls)
	startTime := time.Now()

	for i := range numCalls {
		go func(callIndex int) {
			defer wg.Done()

			data, err := slave.CallForBuffer("echo", payloadData)
			if err != nil {
				log.Printf("Call %d failed: %v\n", callIndex, err)
				return
			}

			if len(data) != common.PayloadSize {
				log.Printf("Call %d: Data size mismatch!\n", callIndex)
				return
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Println("\n--- FastRpc Benchmark Complete ---")
	fmt.Printf("Total time: %v\n", duration)
	fmt.Printf("Total calls: %d\n", numCalls)
	fmt.Printf("Avg. time per call: %v\n", duration/time.Duration(numCalls))
	fmt.Printf("Avg. calls/sec: %.2f\n", float64(numCalls)/duration.Seconds())
}
