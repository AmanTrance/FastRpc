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

type RpcSlaveConfig struct {
	maxRetryAttempts int
	retryAfter       time.Duration
}

type RpcSlave struct {
	closed          bool
	poolSize        int
	masterPort      int
	masterIP        net.IP
	mutex           *sync.RWMutex
	capabilitiesMap map[string]uint32
	connectionPool  chan *ConnectionState
	config          RpcSlaveConfig
}

func NewSlave(masterIP net.IP, masterPort int, poolSize int) (*RpcSlave, error) {

	var slave RpcSlave = RpcSlave{
		poolSize:        poolSize,
		masterPort:      masterPort,
		masterIP:        masterIP,
		mutex:           new(sync.RWMutex),
		capabilitiesMap: make(map[string]uint32),
		connectionPool:  make(chan *ConnectionState, poolSize),
		config: RpcSlaveConfig{
			maxRetryAttempts: 50,
			retryAfter:       time.Millisecond * 200,
		},
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

func (r *RpcSlave) createNewConnection() (*ConnectionState, error) {
	masterConnection, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   r.masterIP,
		Port: r.masterPort,
	})
	if err != nil {
		return nil, err
	}

	masterConnection.SetReadBuffer(BUFFER_SIZE)
	masterConnection.SetWriteBuffer(BUFFER_SIZE)

	return &ConnectionState{
		tcpStream: masterConnection,
		reader:    bufio.NewReaderSize(masterConnection, BUFFER_SIZE),
		writer:    bufio.NewWriterSize(masterConnection, BUFFER_SIZE),
	}, nil
}

func (r *RpcSlave) GetMasterCapabilities() ([]MasterCapabilitiesDTO, error) {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.closed {
		return nil, errors.New("rpc slave is closed")
	}

	connectionState, ok := <-r.connectionPool
	if !ok {
		return nil, errors.New("connection pool closed")
	}

	for range r.config.maxRetryAttempts {

		if connectionState == nil {
			var err error
			connectionState, err = r.createNewConnection()
			if err != nil {
				log.Printf("Connection retry failed: %v", err)
				time.Sleep(r.config.retryAfter)
				continue
			}
		}

		err := writeSpecifiedBytes(connectionState.writer, make([]byte, 12), 12)
		if err == nil {
			err = connectionState.writer.Flush()
		}

		var responseBuf []byte
		if err == nil {
			responseBuf, err = readSpecifiedBytes(connectionState.reader, 9)
		}

		if err != nil {
			connectionState.tcpStream.Close()
			connectionState = nil
			continue
		}

		if (responseBuf[0] & 0b00000001) == 0b00000001 {
			errorBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
			r.connectionPool <- connectionState
			if err != nil {
				return nil, err
			}
			return nil, errors.New(string(errorBuf))
		} else {
			dataBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
			r.connectionPool <- connectionState
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
	return nil, errors.New("failed to retrieve master capabilities after 50 retries")
}

func (r *RpcSlave) CallForBuffer(method string, buf []byte) ([]byte, error) {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.closed {
		return nil, errors.New("rpc slave is closed")
	}

	rpcID, ok := r.capabilitiesMap[method]
	if !ok {
		return nil, errors.New("unknown rpc method: " + method)
	}

	connectionState, ok := <-r.connectionPool
	if !ok {
		return nil, errors.New("connection pool closed")
	}

	for range r.config.maxRetryAttempts {

		if connectionState == nil {
			var err error
			connectionState, err = r.createNewConnection()
			if err != nil {
				log.Printf("Connection retry failed: %v", err)
				time.Sleep(r.config.retryAfter)
				continue
			}
		}

		var headersBuffer []byte = make([]byte, 12)
		binary.BigEndian.PutUint32(headersBuffer[:4], rpcID)
		binary.BigEndian.PutUint64(headersBuffer[4:], uint64(len(buf)))

		err := writeSpecifiedBytes(connectionState.writer, headersBuffer, 12)
		if err == nil {
			err = writeSpecifiedBytes(connectionState.writer, buf, len(buf))
		}
		if err == nil {
			err = connectionState.writer.Flush()
		}

		var responseBuf []byte
		if err == nil {
			responseBuf, err = readSpecifiedBytes(connectionState.reader, 9)
		}

		if err != nil {
			connectionState.tcpStream.Close()
			connectionState = nil
			continue
		}

		if (responseBuf[0] & 0b00000001) == 0b00000001 {
			errorBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
			r.connectionPool <- connectionState
			if err != nil {
				return nil, err
			}
			return nil, errors.New(string(errorBuf))
		} else {
			dataBuf, err := readSpecifiedBytes(connectionState.reader, int(binary.BigEndian.Uint64(responseBuf[1:])))
			r.connectionPool <- connectionState
			if err != nil {
				return nil, err
			}
			return dataBuf, nil
		}
	}
	return nil, errors.New("rpc call failed after 50 retries")
}
