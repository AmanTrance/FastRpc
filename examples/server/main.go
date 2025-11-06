package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	master, err := fastrpc.NewMaster()
	if err != nil {
		log.Fatalf("Failed to create master: %v", err)
	}

	master.RegisterRPC("ping", "Simple ping-pong check", "nil", "string", func(i *fastrpc.IOOperator) error {
		log.Println("[Server] Received PING")
		return i.WriteIOFromBuffer([]byte("pong"))
	})

	master.RegisterRPC("echo", "Echoes back any data sent", "binary", "binary", func(i *fastrpc.IOOperator) error {
		log.Printf("[Server] Received ECHO with %d bytes\n", i.ReadDataLeft())
		data, err := i.ReadIOStream(int(i.ReadDataLeft()))
		if err != nil {
			return err
		}
		return i.WriteIOFromBuffer(data)
	})

	master.RegisterRPC("large_data", "Returns a 10MB payload", "nil", "binary", func(i *fastrpc.IOOperator) error {
		log.Println("[Server] Received LARGE_DATA request")
		const dataSize = 10 * 1024 * 1024
		payload := make([]byte, dataSize)
		rand.Read(payload)

		log.Println("[Server] Sending 10MB payload...")
		return i.WriteIOFromBuffer(payload)
	})

	master.RegisterRPC("force_error", "A test function that always fails", "nil", "nil", func(i *fastrpc.IOOperator) error {
		log.Println("[Server] Received FORCE_ERROR, returning an error")
		return errors.New("a server-side error was forced")
	})

	ip := net.IPv4(127, 0, 0, 1)
	port := 10000
	fmt.Printf("Starting FastRpc Master on %s:%d...\n", ip, port)

	err = master.Start(context.Background(), ip, port)
	if err != nil {
		log.Fatalf("Master failed to start: %v", err)
	}
}
