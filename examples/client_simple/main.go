package main

import (
	"fmt"
	"log"
	"net"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	ip := net.IPv4(127, 0, 0, 1)
	port := 10000
	poolSize := 2

	log.Printf("Connecting to master at %s:%d with pool size %d\n", ip, port, poolSize)
	slave, err := fastrpc.NewSlave(ip, port, poolSize)
	if err != nil {
		log.Fatalf("Failed to create slave: %v", err)
	}
	defer slave.DeInitialize()

	log.Println("Calling RPC: 'ping'...")
	data, err := slave.CallForBuffer("ping", nil)
	if err != nil {
		log.Fatalf("RPC call 'ping' failed: %v", err)
	}

	fmt.Printf("Server replied: %s\n", string(data))

	log.Println("Calling RPC: 'echo'...")
	payload := []byte("Hello from the simple client!")
	data, err = slave.CallForBuffer("echo", payload)
	if err != nil {
		log.Fatalf("RPC call 'echo' failed: %v", err)
	}

	fmt.Printf("Server echoed: %s\n", string(data))
}
