package fastrpc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"sync"
)

type MasterCapabilities struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	IncomingEncoding  string `json:"incomingEncoding"`
	ReturningEncoding string `json:"returningEncoding"`
	rpc               func(*IOOperator, uint64) (uint64, error)
}

type RpcMaster struct {
	counter   uint64
	mutex     sync.Mutex
	discarder io.Writer
	registrar map[uint64]*MasterCapabilities
}

func NewMaster() (*RpcMaster, error) {

	discarder, err := os.Open("/dev/null")
	if err != nil {
		return nil, err
	}

	var master RpcMaster = RpcMaster{
		discarder: discarder,
		registrar: make(map[uint64]*MasterCapabilities),
	}

	master.RegisterRPC(
		"capabilties",
		"Get Master's All Capabilities",
		"json",
		"json",
		func(stream *IOOperator, length uint64) (uint64, error) {
			capabilities, err := master.ShowCapabilities()
			if err != nil {
				return 0, err
			}

			capabilitiesBytes, err := json.Marshal(capabilities)
			if err != nil {
				return 0, err
			}

			err = stream.WriteBuffer(capabilitiesBytes)
			if err != nil {
				return 0, err
			}

			return length, nil
		},
	)

	return &master, nil
}

func (r *RpcMaster) RegisterRPC(name string, description string, incomingEncoding string, returningEncoding string,
	rpc func(*IOOperator, uint64) (uint64, error)) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.registrar[r.counter] = &MasterCapabilities{
		Name:              name,
		Description:       description,
		IncomingEncoding:  incomingEncoding,
		ReturningEncoding: returningEncoding,
		rpc:               rpc,
	}
	r.counter++
}

func (r *RpcMaster) ShowCapabilities() ([]struct {
	RpcID uint64 `json:"rpcId"`
	*MasterCapabilities
}, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	var capabilities []struct {
		RpcID uint64
		*MasterCapabilities
	} = make([]struct {
		RpcID uint64
		*MasterCapabilities
	}, len(r.registrar))

	for id, rpc := range r.registrar {
		capabilities = append(capabilities, struct {
			RpcID uint64
			*MasterCapabilities
		}{id, rpc})
	}

	return []struct {
		RpcID uint64 "json:\"rpcId\""
		*MasterCapabilities
	}(capabilities), nil
}

func (r *RpcMaster) RunRPC(ctx context.Context, ip net.IP, port int) error {

	socket, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   ip,
		Port: port,
	})
	if err != nil {
		return err
	}

	defer socket.Close()

loop:
	for {
		select {

		case <-ctx.Done():
			break loop

		default:
			tcpStream, err := socket.AcceptTCP()
			if err != nil {
				return err
			}

			go func() {
				defer tcpStream.Close()

				protocolError := tcpStream.SetReadBuffer(1024 * 1024)
				if protocolError != nil {
					return
				}

				protocolError = tcpStream.SetKeepAlive(true)
				if protocolError != nil {
					return
				}

				for {
					rpcIDBuffer, protocolError := readSpecifiedBytes(tcpStream, 8)
					if protocolError != nil {
						return
					}

					lengthBuffer, protocolError := readSpecifiedBytes(tcpStream, 8)
					if protocolError != nil {
						return
					}

					capability, ok := r.registrar[binary.BigEndian.Uint64(rpcIDBuffer)]
					if !ok {
						return
					}

					bytesLeft, protocolError := capability.rpc(&IOOperator{tcpStream}, binary.BigEndian.Uint64(lengthBuffer))
					if protocolError != nil {
						return
					}

					if bytesLeft != 0 {
						protocolError = r.Discard(tcpStream, int64(bytesLeft))
						if protocolError != nil {
							return
						}
					}
				}
			}()
		}
	}

	return nil
}

func readSpecifiedBytes(tcpStream *net.TCPConn, bytesCount int) ([]byte, error) {

	var bytesBuffer []byte = make([]byte, bytesCount)
	for {
		var tempBuffer []byte = make([]byte, bytesCount)
		bytesRead, err := tcpStream.Read(bytesBuffer)
		if err != nil {
			return nil, err
		}

		if bytesRead < bytesCount {
			bytesCount -= bytesRead
		}

		bytesBuffer = append(bytesBuffer, tempBuffer[:bytesRead]...)

		if bytesCount == 0 {
			break
		}
	}

	return bytesBuffer, nil
}
