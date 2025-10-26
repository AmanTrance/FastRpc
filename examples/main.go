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

	go func() {
		err = master.RunRPC(context.Background(), net.IPv4(127, 0, 0, 1), 10000)
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

	fmt.Printf("%v", c)
}
