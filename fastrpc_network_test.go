package fastrpc_test

import (
	"bytes"
	"crypto/rand"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	fastrpc "github.com/AmanTrance/FastRpc"
	"github.com/stretchr/testify/assert"
)

var (
	remoteMasterIP   net.IP
	remoteMasterPort int
	configLoaded     bool
	skipTests        bool
)

func getRemoteMasterConfig(t *testing.T) {
	if configLoaded {
		if skipTests {
			t.Skip("Skipping network test: FASTRPC_MASTER_ADDR environment variable is not set.")
		}
		return
	}
	configLoaded = true

	masterAddr := os.Getenv("FASTRPC_MASTER_ADDR")
	if masterAddr == "" {
		skipTests = true
		t.Skip("Skipping network test: FASTRPC_MASTER_ADDR environment variable is not set.")
		return
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Failed to resolve master address '%s': %v", masterAddr, err)
	}
	remoteMasterIP = tcpAddr.IP
	remoteMasterPort = tcpAddr.Port
	log.Printf("Running network tests against remote master: %s", masterAddr)
}

func TestNetwork_BasicCall(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 1)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	data, err := slave.CallForBuffer("ping", nil)
	if !assertions.NoError(err) {
		return
	}
	assertions.Equal("pong", string(data))

	payload := []byte("hello world")
	data, err = slave.CallForBuffer("echo", payload)
	if !assertions.NoError(err) {
		return
	}
	assertions.Equal(payload, data)
}

func TestNetwork_SequentialCallsOnSameConnection(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 1)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	for i := range 100 {
		payload := []byte("hello " + strconv.Itoa(i))
		data, err := slave.CallForBuffer("echo", payload)

		if !assertions.NoError(err, "Call failed at iteration %d", i) {
			return
		}
		assertions.Equal(payload, data, "Data mismatch at iteration %d", i)
	}
}

func TestNetwork_ConcurrentCalls(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 20)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	numCalls := 1000
	var wg sync.WaitGroup
	wg.Add(numCalls)

	for i := range numCalls {
		callIndex := i
		go func() {
			defer wg.Done()
			payload := []byte("concurrent hello " + strconv.Itoa(callIndex))
			data, err := slave.CallForBuffer("echo", payload)

			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, payload, data)
		}()
	}
	wg.Wait()
}

func TestNetwork_LargeData(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 1)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	const dataSize = 10 * 1024 * 1024
	payload := make([]byte, dataSize)
	_, err = rand.Read(payload)
	if !assertions.NoError(err, "Failed to generate random payload") {
		return
	}

	data, err := slave.CallForBuffer("echo", payload)
	if !assertions.NoError(err) {
		return
	}

	assertions.Equal(len(payload), len(data), "Returned data size is wrong")
	assertions.True(bytes.Equal(payload, data), "Returned data does not match sent data")
}

func TestNetwork_MultipleSlaves(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	numSlaves := 10
	numCallsPerSlave := 100
	var wg sync.WaitGroup
	wg.Add(numSlaves * numCallsPerSlave)

	for i := range numSlaves {
		slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 5)
		if !assertions.NoError(err) {
			t.Logf("Failed to create slave %d", i)
			wg.Add(-numCallsPerSlave)
			continue
		}
		defer slave.DeInitialize()

		go func(slaveIndex int) {
			for j := range numCallsPerSlave {
				payload := []byte("slave " + strconv.Itoa(slaveIndex) + " call " + strconv.Itoa(j))
				go func(p []byte) {
					defer wg.Done()
					data, err := slave.CallForBuffer("echo", p)
					assert.NoError(t, err)
					assert.Equal(t, p, data)
				}(payload)
			}
		}(i)
	}
	wg.Wait()
}

func TestNetwork_UnknownRPC(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 1)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	data, err := slave.CallForBuffer("non_existent_rpc", nil)

	if !assertions.Error(err, "An error is expected for an unknown RPC") {
		return
	}
	assertions.Nil(data, "Data should be nil when an error occurs")
	assertions.ErrorContains(err, "unknown rpc method non_existent_rpc")
}

func TestNetwork_ServerDiscardLogic(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 1)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	payload := make([]byte, 1024*1024)
	data, err := slave.CallForBuffer("discard", payload)
	if !assertions.NoError(err) {
		return
	}
	assertions.Equal("discarded", string(data))

	data, err = slave.CallForBuffer("ping", nil)
	if !assertions.NoError(err, "The second call failed, server discard logic is broken") {
		return
	}
	assertions.Equal("pong", string(data), "The second call returned wrong data")
}

func TestNetwork_ServerRepliesWithServerError(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 1)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	data, err := slave.CallForBuffer("force_error", nil)

	if !assertions.Error(err, "Expected an error from the server") {
		return
	}
	assertions.Nil(data, "Data should be nil on error")
	assertions.ErrorContains(err, "this is a forced server error")
}

func TestNetwork_DeInitialize(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 5)
	if !assertions.NoError(err) {
		return
	}

	slave.DeInitialize()

	_, err = slave.CallForBuffer("ping", nil)
	assertions.Error(err, "Call should fail after DeInitialize")
}

func TestNetwork_SlavePerformanceBottleneck(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 10)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	numCalls := 10
	var wg sync.WaitGroup
	wg.Add(numCalls)

	startTime := time.Now()
	for i := range numCalls {
		go func(i int) {
			defer wg.Done()
			_, err := slave.CallForBuffer("ping_slow", nil)
			assert.NoError(t, err, "Call %d failed", i)
		}(i)
	}
	wg.Wait()
	duration := time.Since(startTime)

	assertions.True(duration < 100*time.Millisecond,
		"PERFORMANCE BUG: Calls were serialized. Expected < 100ms, took %v", duration)
}

func TestNetwork_LargeDataStress(t *testing.T) {
	getRemoteMasterConfig(t)
	assertions := assert.New(t)

	slave, err := fastrpc.NewSlave(remoteMasterIP, remoteMasterPort, 5)
	if !assertions.NoError(err) {
		return
	}
	defer slave.DeInitialize()

	const dataSize = 20 * 1024 * 1024
	const numCalls = 1000

	payload := make([]byte, dataSize)
	_, err = rand.Read(payload)
	if !assertions.NoError(err, "Failed to generate random payload") {
		return
	}

	var wg sync.WaitGroup
	wg.Add(numCalls)

	log.Printf("Starting TestNetwork_LargeDataStress: %d calls with %d MB payload...\n", numCalls, dataSize/(1024*1024))

	startTime := time.Now()

	for i := range numCalls {
		go func(i int) {
			defer wg.Done()

			data, err := slave.CallForBuffer("echo", payload)
			if !assertions.NoError(err, "Call %d failed", i) {
				return
			}
			if !assertions.Equal(len(payload), len(data), "Returned data size is wrong on call %d", i) {
				return
			}
			if i%10 == 0 {
				log.Printf("...Stress test call %d complete", i)
			}
		}(i)
	}

	wg.Wait()

	duration := time.Since(startTime)

	log.Printf("--- TestNetwork_LargeDataStress Complete ---")
	log.Printf("Total time for %d calls: %v", numCalls, duration)
	log.Printf("Avg. time per call: %v", duration/time.Duration(numCalls))
	log.Printf("Avg. calls/sec: %.2f\n", float64(numCalls)/duration.Seconds())
}
