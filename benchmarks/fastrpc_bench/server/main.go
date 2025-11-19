package main

import (
	"context"
	"log"
	"net"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	master, err := fastrpc.NewMaster()
	if err != nil {
		log.Fatalf("Failed to create master: %v", err)
	}

	master.RegisterRPC("echo", "Echoes back any data sent", "binary", "binary", func(i *fastrpc.IOOperator) error {
		data, err := i.ReadIOStream(int(i.ReadDataLeft()))
		if err != nil {
			return err
		}
		return i.WriteIOFromBuffer(data)
	})

	ip := net.IPv4(127, 0, 0, 1)
	port := 10000
	log.Printf("Starting FastRpc Master on %s:%d...", ip, port)

	err = master.Start(context.Background(), ip, port)
	if err != nil {
		log.Fatalf("Master failed to start: %v", err)
	}
}
