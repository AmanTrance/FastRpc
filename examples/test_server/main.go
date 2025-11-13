package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"time"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func registerTestRPCs(master *fastrpc.RpcMaster) {
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

	master.RegisterRPC("ping_slow", "returns pong after 20ms", "text", "text", func(i *fastrpc.IOOperator) error {
		time.Sleep(20 * time.Millisecond)
		return i.WriteIOFromBuffer([]byte("pong"))
	})

	log.Println("Successfully registered all test RPCs: ping, echo, discard, force_error, ping_slow")
}

func main() {
	master, err := fastrpc.NewMaster()
	if err != nil {
		log.Fatalf("Failed to create new master: %v", err)
	}

	registerTestRPCs(master)

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = "0.0.0.0:10000"
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to resolve listen address '%s': %v", listenAddr, err)
	}

	ip := tcpAddr.IP
	port := tcpAddr.Port

	log.Printf("Starting FastRpc Test Master on %s:%d...", ip.String(), port)

	err = master.Start(context.Background(), ip, port)
	if err != nil {
		log.Fatalf("Master failed to start: %v", err)
	}
}
