# FastRpc

[![Go Reference](https://pkg.go.dev/badge/github.com/AmanTrance/FastRpc.svg)](https://pkg.go.dev/github.com/AmanTrance/FastRpc)
[![Tests](https://github.com/AmanTrance/FastRpc/actions/workflows/go-tests.yml/badge.svg)](https://github.com/AmanTrance/FastRpc/actions/workflows/go-tests.yml)

**FastRpc** is a simple, high-performance, TCP-based binary RPC (Remote Procedure Call) framework for Go. It is designed for extremely low-latency, synchronous, server-to-server communication where a full-featured framework like gRPC is overkill and text-based protocols like JSON-RPC are too slow.

## Why FastRpc?

In many systems, you need a service to synchronously call another service and get a response *fast*. The problem is that many solutions are either too complex or too slow:

* **HTTP/JSON-RPC:** Easy to use, but the overhead of text parsing (JSON) and HTTP handshakes is high, making sub-10ms response times difficult.
* **gRPC:** Extremely fast and robust, but requires `.proto` files, code generation, and a more complex setup.
* **Polling/Queues (like RabbitMQ):** This is an *asynchronous* pattern. It's great for background jobs, but not for a synchronous "call-and-wait" request.

FastRpc solves the "call-and-wait" problem by providing:
1.  **A Simple Binary Protocol:** No text parsing. It sends raw bytes over a custom header protocol.
2.  **Persistent Connection Pooling:** Clients (Slaves) hold a pool of "on-hold" TCP connections. When you make a call, it uses an existing connection, skipping the slow TCP handshake.
3.  **Lightweight API:** The entire framework is just a few Go files with a minimal, easy-to-understand API.

## Features

* **`RpcMaster` (Server):**
    * Concurrently handles thousands of client connections, one goroutine per client.
    * Simple `RegisterRPC` method to map a string name to a handler function.
    * Graceful `Close()` method.
* **`RpcSlave` (Client):**
    * Manages a pool of persistent TCP connections for high throughput.
    * Simple `CallForBuffer` method that feels like a local function call.
    * Automatically re-uses connections from the pool.
* **Safe & Robust:**
    * Concurrent-safe server and client using `sync.Mutex`.
    * Properly handles stream I/O using loops to ensure all bytes are read/written.
    * Automatically discards unread data to prevent connection poisoning.

## Installation

```sh
go get github.com/AmanTrance/FastRpc
````

## Quick Start

Here is a complete, minimal example.

### 1\. Server (`server.go`)

```go
package main

import (
	"context"
	"log"
	"net"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	master, _ := fastrpc.NewMaster()

	// Register a "ping" RPC
	master.RegisterRPC("ping", "ping-pong", "", "", func(i *fastrpc.IOOperator) error {
		return i.WriteIOFromBuffer([]byte("pong"))
	})

	log.Println("Starting RPC server on :10000")
	// Start blocks and listens for connections
	master.Start(context.Background(), net.IPv4(127, 0, 0, 1), 10000)
}
```

### 2\. Client (`client.go`)

```go
package main

import (
	"fmt"
	"log"
	"net"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	// Connect to the master, creating a pool of 5 connections
	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), 10000, 5)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer slave.DeInitialize()

	// Call the "ping" RPC
	data, err := slave.CallForBuffer("ping", nil)
	if err != nil {
		log.Fatalf("RPC call failed: %v", err)
	}

	fmt.Printf("Server responded: %s\n", string(data)) // Prints: Server responded: pong
}
```

## API Reference

### `RpcMaster`

```go
// Creates a new, ready-to-configure RpcMaster.
// Automatically registers a 'capabilities' RPC at ID 0.
func NewMaster() (*RpcMaster, error)

// Registers a new function to be called by clients.
func (r *RpcMaster) RegisterRPC(name string, description string, incomingType string, returningType string, rpc func(*IOOperator) error)

// Starts the server. This is a blocking call.
func (r *RpcMaster) Start(ctx context.Context, ip net.IP, port int) error

// Closes the master's listening socket, stopping it from accepting new connections.
func (r *RpcMaster) Close() error
```

### `RpcSlave`

```go
// Creates a new client and connects to the master, filling the connection pool.
func NewSlave(masterIP net.IP, masterPort int, poolSize int) (*RpcSlave, error)

// Gets the list of available RPCs from the master.
func (r *RpcSlave) GetMasterCapabilities() ([]MasterCapabilitiesDTO, error)

// Calls an RPC by name and waits for a response. This is the primary method.
func (r *RpcSlave) CallForBuffer(method string, buf []byte) ([]byte, error)

// Closes all connections in the pool and shuts down the slave.
func (r *RpcSlave) DeInitialize()
```

### `IOOperator`

(This is the object passed into your RPC handler function)

```go
// Returns the number of bytes the client sent that have not been read yet.
func (i *IOOperator) ReadDataLeft() uint64

// Reads a specific number of bytes from the client's request payload.
func (i *IOOperator) ReadIOStream(count int) ([]byte, error)

// Writes a byte slice as the response.
// This will send a 9-byte header followed by the data.
func (i *IOOperator) WriteIOFromBuffer(buf []byte) error

// Writes data from a reader (e.g., a file) as the response.
func (i *IOOperator) WriteIOFromReader(reader io.Reader, count int, chunkSize int) error

// Sends an empty, successful response.
func (i *IOOperator) WriteNothing() error

// Sends an error response. The client will receive this as an error.
func (i *IOOperator) WriteError(message string) error
```

## Running Tests

To run the full test suite, including concurrency and data integrity tests:

```sh
go test -v ./...
```

## License

This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.

```