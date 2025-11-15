package fastrpc

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type ConnectionState struct {
	tcpStream *net.TCPConn
	reader    *bufio.Reader
	writer    *bufio.Writer
}

type RpcSlave struct {
	closed          bool
	poolSize        int
	masterPort      int
	masterIP        net.IP
	mutex           *sync.RWMutex
	capabilitiesMap map[string]uint32
	connectionPool  chan *ConnectionState
}

func NewSlave(masterIP net.IP, masterPort int, poolSize int) (*RpcSlave, error) {

	var slave RpcSlave = RpcSlave{
		poolSize:        poolSize,
		masterPort:      masterPort,
		masterIP:        masterIP,
		mutex:           new(sync.RWMutex),
		capabilitiesMap: make(map[string]uint32),
		connectionPool:  make(chan *ConnectionState, poolSize),
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

		masterConnection.SetReadBuffer(BUFFER_SIZE)
		masterConnection.SetWriteBuffer(BUFFER_SIZE)

		slave.connectionPool <- &ConnectionState{
			tcpStream: masterConnection,
			reader:    bufio.NewReaderSize(masterConnection, BUFFER_SIZE),
			writer:    bufio.NewWriterSize(masterConnection, BUFFER_SIZE),
		}
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

	for connectionState := range r.connectionPool {
		connectionState.tcpStream.Close()
	}

	r.closed = true
}

func (r *RpcSlave) GetMasterCapabilities() ([]MasterCapabilitiesDTO, error) {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.closed {
		return nil, errors.New("rpc slave is closed")
	}

	var err error
	connectionState := <-r.connectionPool
	defer func() {
		if err != nil {
			connectionState.tcpStream.Close()

		connectionRetry:
			masterConnection, connectionErr := net.DialTCP("tcp", nil, &net.TCPAddr{
				IP:   r.masterIP,
				Port: r.masterPort,
			})
			if connectionErr != nil {
				log.Default().Printf("Error: %s\n", err.Error())
				time.Sleep(time.Second * 5)
				goto connectionRetry
			}

			masterConnection.SetReadBuffer(BUFFER_SIZE)
			masterConnection.SetWriteBuffer(BUFFER_SIZE)

			r.connectionPool <- &ConnectionState{
				tcpStream: masterConnection,
				reader:    bufio.NewReaderSize(masterConnection, BUFFER_SIZE),
				writer:    bufio.NewWriterSize(masterConnection, BUFFER_SIZE),
			}
		} else {
			r.connectionPool <- connectionState
		}
	}()

	err = writeSpecifiedBytes(connectionState.writer, make([]byte, 12), 12)
	if err != nil {
		return nil, err
	}

	err = connectionState.writer.Flush()
	if err != nil {
		return nil, err
	}

	responseBuf, err := readSpecifiedBytes(connectionState.reader, 9)
	if err != nil {
		return nil, err
	}

	if (responseBuf[0] & 0b00000001) == 0b00000001 {
		errorBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(errorBuf))
	} else {
		dataBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
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
	defer r.mutex.RUnlock()

	if r.closed {
		return nil, errors.New("rpc slave is closed")
	}

	rpcID, ok := r.capabilitiesMap[method]
	if !ok {
		return nil, errors.New("unknown rpc method " + method)
	}

	var err error
	connectionState := <-r.connectionPool
	defer func() {
		if err != nil {
			connectionState.tcpStream.Close()

		connectionRetry:
			masterConnection, connectionErr := net.DialTCP("tcp", nil, &net.TCPAddr{
				IP:   r.masterIP,
				Port: r.masterPort,
			})
			if connectionErr != nil {
				log.Default().Printf("Error: %s\n", err.Error())
				time.Sleep(time.Second * 5)
				goto connectionRetry
			}

			masterConnection.SetReadBuffer(BUFFER_SIZE)
			masterConnection.SetWriteBuffer(BUFFER_SIZE)

			r.connectionPool <- &ConnectionState{
				tcpStream: masterConnection,
				reader:    bufio.NewReaderSize(masterConnection, BUFFER_SIZE),
				writer:    bufio.NewWriterSize(masterConnection, BUFFER_SIZE),
			}
		} else {
			r.connectionPool <- connectionState
		}
	}()

	var headersBuffer []byte = make([]byte, 12)
	binary.BigEndian.PutUint32(headersBuffer[:4], rpcID)
	binary.BigEndian.PutUint64(headersBuffer[4:], uint64(len(buf)))
	err = writeSpecifiedBytes(connectionState.writer, headersBuffer, 12)
	if err != nil {
		return nil, err
	}

	err = writeSpecifiedBytes(connectionState.writer, buf, len(buf))
	if err != nil {
		return nil, err
	}

	err = connectionState.writer.Flush()
	if err != nil {
		return nil, err
	}

	responseBuf, err := readSpecifiedBytes(connectionState.reader, 9)
	if err != nil {
		return nil, err
	}

	if (responseBuf[0] & 0b00000001) == 0b00000001 {
		errorBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(errorBuf))
	} else {
		dataBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
		if err != nil {
			return nil, err
		}

		return dataBuf, nil
	}
}
