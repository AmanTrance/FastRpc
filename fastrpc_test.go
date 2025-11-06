package fastrpc_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	fastrpc "github.com/AmanTrance/FastRpc"
	"github.com/stretchr/testify/assert"
)

func setupMaster(t *testing.T) (master *fastrpc.RpcMaster, port int, teardown func()) {
	assertions := assert.New(t)

	master, err := fastrpc.NewMaster()
	if !assertions.NoError(err, "Failed to create new master") {
		t.FailNow()
	}

	master.RegisterRPC("ping", "returns pong", "text", "text", func(i *fastrpc.IOOperator) error {
		return i.WriteIOFromBuffer([]byte("pong"))
	})

	master.RegisterRPC("echo", "returns input", "binary", "binary", func(i *fastrpc.IOOperator) error {
		data, err := i.ReadIOStream(int(i.ReadDataLeft()))
		if err != nil {
			return err
		}
		return i.WriteIOFromBuffer(data)
	})

	master.RegisterRPC("discard", "reads nothing, returns nothing", "binary", "text", func(i *fastrpc.IOOperator) error {
		return i.WriteIOFromBuffer([]byte("discarded"))
	})

	master.RegisterRPC("force_error", "returns an error", "", "", func(i *fastrpc.IOOperator) error {
		return errors.New("this is a forced server error")
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if !assertions.NoError(err) {
		t.FailNow()
	}

	port = listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		master.Start(ctx, net.IPv4(127, 0, 0, 1), port)
	}()

	time.Sleep(50 * time.Millisecond)

	teardown = func() {
		cancel()
		master.Close()
	}

	return master, port, teardown
}

func TestBasicCall(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 1)
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

func TestSequentialCallsOnSameConnection(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 1)
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

func TestConcurrentCalls(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 20)
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

func TestLargeData(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 1)
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

func TestMultipleSlaves(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	numSlaves := 10
	numCallsPerSlave := 100
	var wg sync.WaitGroup
	wg.Add(numSlaves * numCallsPerSlave)

	for i := range numSlaves {
		slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 5)
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

func TestUnknownRPC(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 1)
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

func TestServerDiscardLogic(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 1)
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

func TestServerRepliesWithServerError(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 1)
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

func TestDeInitialize(t *testing.T) {
	assertions := assert.New(t)
	_, port, teardown := setupMaster(t)
	defer teardown()

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), port, 5)
	if !assertions.NoError(err) {
		return
	}

	slave.DeInitialize()

	_, err = slave.CallForBuffer("ping", nil)
	assertions.Error(err, "Call should fail after DeInitialize")
}
