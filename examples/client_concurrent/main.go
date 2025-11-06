package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	ip := net.IPv4(127, 0, 0, 1)
	port := 10000
	poolSize := 20
	numCalls := 1000

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

			payload := []byte("Hello from call " + strconv.Itoa(callIndex))

			data, err := slave.CallForBuffer("echo", payload)
			if err != nil {
				log.Printf("Call %d failed: %v\n", callIndex, err)
				return
			}

			if string(data) != string(payload) {
				log.Printf("Call %d: Data mismatch!\n", callIndex)
				return
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Println("\n--- All calls complete ---")
	fmt.Printf("Total time: %v\n", duration)
	fmt.Printf("Total calls: %d\n", numCalls)
	fmt.Printf("Avg. calls/sec: %.2f\n", float64(numCalls)/duration.Seconds())
}
