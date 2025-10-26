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

type MasterCapabilitiesDTO struct {
	RpcID             uint32 `json:"rpcId"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	IncomingEncoding  string `json:"incomingEncoding"`
	ReturningEncoding string `json:"returningEncoding"`
}

type RpcMaster struct {
	counter   uint32
	mutex     sync.Mutex
	discarder io.Writer
	registrar map[uint32]*MasterCapabilities
}

func NewMaster() (*RpcMaster, error) {

	discarder, err := os.OpenFile("/dev/null", os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	var master RpcMaster = RpcMaster{
		discarder: discarder,
		registrar: make(map[uint32]*MasterCapabilities),
	}

	master.RegisterRPC(
		"capabilities",
		"Get Master's All Capabilities",
		"application/json",
		"application/json",
		func(stream *IOOperator) error {
			capabilities, err := master.ShowCapabilities()
			if err != nil {
				return err
			}

			capabilitiesBytes, err := json.Marshal(&capabilities)
			if err != nil {
				return err
			}

			return stream.WriteIOFromBuffer(capabilitiesBytes)
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

func (r *RpcMaster) ShowCapabilities() ([]MasterCapabilitiesDTO, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	var capabilities []MasterCapabilitiesDTO = make([]MasterCapabilitiesDTO, 0)

	for id, rpc := range r.registrar {
		capabilities = append(capabilities, MasterCapabilitiesDTO{
			RpcID:             id,
			Name:              rpc.Name,
			Description:       rpc.Description,
			IncomingEncoding:  rpc.IncomingEncoding,
			ReturningEncoding: rpc.ReturningEncoding,
		})
	}

	return capabilities, nil
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
					headersBuffer, streamError := readSpecifiedBytes(tcpStream, 12)
					if streamError != nil {
						return
					}

					ioStream := NewIOOperator(tcpStream, binary.BigEndian.Uint64(headersBuffer[4:]))

					capability, ok := r.registrar[binary.BigEndian.Uint32(headersBuffer[:4])]
					if !ok {
						streamError = ioStream.WriteError("rpc not found")
						if streamError != nil {
							return
						}

						if ioStream.leftLength != 0 {
							streamError = discard(tcpStream, r.discarder, ioStream.leftLength)
							if streamError != nil {
								return
							}
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
