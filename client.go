package fastrpc

import (
	"net"
	"sync"
)

type RpcSlave struct {
	closed         bool
	poolSize       int
	masterPort     int
	mutex          sync.Mutex
	masterIP       net.IP
	connectionPool chan *net.TCPConn
}

func NewRpcSlave(masterIP net.IP, masterPort int) (*RpcSlave, error) {

	return &RpcSlave{
		masterIP:   masterIP,
		masterPort: masterPort,
	}, nil
}

func (r *RpcSlave) InitializePool(poolSize int) error {

	r.connectionPool = make(chan *net.TCPConn, poolSize)

	for range poolSize {
		masterConnection, err := net.DialTCP("tcp", nil, &net.TCPAddr{
			IP:   r.masterIP,
			Port: r.masterPort,
		})
		if err != nil {
			return err
		}

		r.connectionPool <- masterConnection
	}

	return nil
}

func (r *RpcSlave) DeInitialize() {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	close(r.connectionPool)

	for masterConnection := range r.connectionPool {
		masterConnection.Close()
	}
}
