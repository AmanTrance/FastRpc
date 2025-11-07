package fastrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"sync"
)

type RpcSlave struct {
	closed          bool
	poolSize        int
	masterPort      int
	masterIP        net.IP
	mutex           *sync.RWMutex
	capabilitiesMap map[string]uint32
	connectionPool  chan *net.TCPConn
}

func NewSlave(masterIP net.IP, masterPort int, poolSize int) (*RpcSlave, error) {

	var slave RpcSlave = RpcSlave{
		poolSize:        poolSize,
		masterPort:      masterPort,
		masterIP:        masterIP,
		mutex:           new(sync.RWMutex),
		capabilitiesMap: make(map[string]uint32),
		connectionPool:  make(chan *net.TCPConn, poolSize),
	}

	for range poolSize {
		masterConnection, err := net.DialTCP("tcp", nil, &net.TCPAddr{
			IP:   masterIP,
			Port: masterPort,
		})
		if err != nil {
			slave.DeInitialize()
			return nil, err
		}

		slave.connectionPool <- masterConnection
	}

	masterCapabilities, err := slave.GetMasterCapabilities()
	if err != nil {
		slave.DeInitialize()
		return nil, err
	}

	for i := range masterCapabilities {
		slave.capabilitiesMap[masterCapabilities[i].Name] = masterCapabilities[i].RpcID
	}

	return &slave, nil
}

func (r *RpcSlave) DeInitialize() {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return
	}

	clear(r.capabilitiesMap)
	close(r.connectionPool)

	for masterConnection := range r.connectionPool {
		masterConnection.Close()
	}

	r.closed = true
}

func (r *RpcSlave) GetMasterCapabilities() ([]MasterCapabilitiesDTO, error) {

	r.mutex.RLock()
	if r.closed {
		return nil, errors.New("rpc slave is closed")
	}
	defer r.mutex.RUnlock()

	connection := <-r.connectionPool
	defer func() {
		r.connectionPool <- connection
	}()

	err := writeSpecifiedBytes(connection, make([]byte, 12), 12)
	if err != nil {
		return nil, err
	}

	responseBuf, err := readSpecifiedBytes(connection, 9)
	if err != nil {
		return nil, err
	}

	if (responseBuf[0] & 0b00000001) == 0b00000001 {
		errorBuf, err := readSpecifiedBytes(connection, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(errorBuf))
	} else {
		dataBuf, err := readSpecifiedBytes(connection, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		var capabilities []MasterCapabilitiesDTO

		err = json.Unmarshal(dataBuf, &capabilities)
		if err != nil {
			return nil, err
		}

		return capabilities, nil
	}
}

func (r *RpcSlave) CallForBuffer(method string, buf []byte) ([]byte, error) {

	r.mutex.RLock()
	if r.closed {
		return nil, errors.New("rpc slave is closed")
	}
	defer r.mutex.RUnlock()

	rpcID, ok := r.capabilitiesMap[method]
	if !ok {
		return nil, errors.New("unknown rpc method " + method)
	}

	connection := <-r.connectionPool
	defer func() {
		r.connectionPool <- connection
	}()

	var headersBuffer []byte = make([]byte, 12)
	binary.BigEndian.PutUint32(headersBuffer[:4], rpcID)
	binary.BigEndian.PutUint64(headersBuffer[4:], uint64(len(buf)))
	err := writeSpecifiedBytes(connection, headersBuffer, 12)
	if err != nil {
		return nil, err
	}

	err = writeSpecifiedBytes(connection, buf, len(buf))
	if err != nil {
		return nil, err
	}

	responseBuf, err := readSpecifiedBytes(connection, 9)
	if err != nil {
		return nil, err
	}

	if (responseBuf[0] & 0b00000001) == 0b00000001 {
		errorBuf, err := readSpecifiedBytes(connection, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(errorBuf))
	} else {
		dataBuf, err := readSpecifiedBytes(connection, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		return dataBuf, nil
	}
}
