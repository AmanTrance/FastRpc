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
	rpc               func(*IOOperator) error
}

type RpcMaster struct {
	counter   uint64
	mutex     sync.Mutex
	discarder io.Writer
	registrar map[uint64]*MasterCapabilities
}

func NewMaster() (*RpcMaster, error) {

	discarder, err := os.OpenFile("/dev/null", os.O_RDWR, 0600)
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
		func(stream *IOOperator) error {
			capabilities, err := master.ShowCapabilities()
			if err != nil {
				return err
			}

			_, err = json.Marshal(capabilities)
			if err != nil {
				return err
			}

			return nil
		},
	)

	return &master, nil
}

func (r *RpcMaster) RegisterRPC(name string, description string, incomingEncoding string, returningEncoding string,
	rpc func(*IOOperator) error) {

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

				streamError := tcpStream.SetReadBuffer(1024 * 1024)
				if streamError != nil {
					return
				}

				streamError = tcpStream.SetKeepAlive(true)
				if streamError != nil {
					return
				}

				for {
					headersBuffer, streamError := readSpecifiedBytes(tcpStream, 16)
					if streamError != nil {
						return
					}

					ioStream := NewIOOperator(tcpStream, binary.BigEndian.Uint64(headersBuffer[8:]))

					capability, ok := r.registrar[binary.BigEndian.Uint64(headersBuffer[:8])]
					if !ok {
						streamError = ioStream.WriteError("rpc not found")
						if streamError != nil {
							return
						}

						streamError = discard(tcpStream, r.discarder, ioStream.leftLength)
						if streamError != nil {
							return
						}

						continue
					}

					rpcError := capability.rpc(ioStream)
					if rpcError != nil && !ioStream.written {
						streamError = ioStream.WriteError(rpcError.Error())
						if streamError != nil {
							return
						}
					}

					if ioStream.leftLength != 0 {
						streamError = discard(tcpStream, r.discarder, ioStream.leftLength)
						if streamError != nil {
							return
						}
					}

					if !ioStream.written {
						streamError = ioStream.WriteNothing()
						if streamError != nil {
							return
						}
					}
				}
			}()
		}
	}

	return nil
}
