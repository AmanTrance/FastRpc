package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/AmanTrance/FastRpc/benchmarks/common"
)

const numCalls = 1000

func main() {
	payloadData := common.GeneratePayload()

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	log.Printf("Dispatching %d concurrent HTTP calls with %d MB payload...\n", numCalls, common.PayloadSize/(1024*1024))

	var wg sync.WaitGroup
	wg.Add(numCalls)

	startTime := time.Now()

	for i := range numCalls {
		go func(callIndex int) {
			defer wg.Done()

			reqBody := bytes.NewReader(payloadData)

			req, err := http.NewRequest("POST", "http://localhost:50053/echo", reqBody)
			if err != nil {
				log.Printf("Call %d failed to create request: %v", callIndex, err)
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Call %d failed: %v", callIndex, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Call %d failed with status: %s", callIndex, resp.Status)
				return
			}

			respBody, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Call %d failed to read response: %v", callIndex, err)
				return
			}

			if len(respBody) != common.PayloadSize {
				log.Printf("Call %d: Data size mismatch! Sent %d, got %d", callIndex, common.PayloadSize, len(respBody))
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Println("\n--- HTTP Benchmark Complete ---")
	fmt.Printf("Total time: %v\n", duration)
	fmt.Printf("Total calls: %d\n", numCalls)
	fmt.Printf("Avg. time per call: %v\n", duration/time.Duration(numCalls))
	fmt.Printf("Avg. calls/sec: %.2f\n", float64(numCalls)/duration.Seconds())
}
