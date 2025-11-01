package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	fastrpc "github.com/AmanTrance/FastRpc"
)

func main() {
	master, err := fastrpc.NewMaster()
	if err != nil {
		log.Default().Fatal(err.Error())
	}

	master.RegisterRPC("rpc1", "hello world", "utf-8", "utf-8", func(i *fastrpc.IOOperator) error {
		return i.WriteIOFromBuffer([]byte("hello world"))
	})

	master.RegisterRPC("rpc2", "data", "utf-8", "utf-8", func(i *fastrpc.IOOperator) error {
		return i.WriteIOFromBuffer([]byte("some random data or some specific encoding based data"))
	})

	go func() {
		err = master.Start(context.Background(), net.IPv4(127, 0, 0, 1), 10000)
		if err != nil {
			log.Default().Fatal(err.Error())
		}
	}()

	time.Sleep(time.Second * 2)

	slave, err := fastrpc.NewSlave(net.IPv4(127, 0, 0, 1), 10000, 2)
	if err != nil {
		log.Default().Fatal(err.Error())
	}

	c, err := slave.GetMasterCapabilities()
	if err != nil {
		log.Default().Fatal(err.Error())
	}

	fmt.Printf("%v\n", c)

	data, err := slave.CallForBuffer("rpc1", nil)
	if err != nil {
		log.Default().Fatal(err.Error())
	}

	println(string(data))

	data, err = slave.CallForBuffer("rpc2", nil)
	if err != nil {
		log.Default().Fatal(err.Error())
	}

	println(string(data))
}
