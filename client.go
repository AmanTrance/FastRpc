package fastrpc

import (
	"net"
	"sync"
)

type MasterCapabilitiesDTO struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	IncomingEncoding  string `json:"incomingEncoding"`
	ReturningEncoding string `json:"returningEncoding"`
}

type RpcSlave struct {
	closed             bool
	poolSize           int
	masterPort         int
	masterIP           net.IP
	mutex              sync.Mutex
	capabilitiesMap    map[string]uint64
	masterCapabilities map[uint64]*MasterCapabilitiesDTO
	connectionPool     chan *net.TCPConn
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

func (r *RpcSlave) GetMasterCapabilities() {

	// connection := <- r.connectionPool

}
