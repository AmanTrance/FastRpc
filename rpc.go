package fastrpc

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type RpcMaster struct {
	counter   uint32
	socket    *net.TCPListener
	mutex     *sync.Mutex
	discarder *os.File
	registrar map[uint32]*MasterCapabilities
}

func NewMaster() (*RpcMaster, error) {

	discarder, err := os.OpenFile("/dev/null", os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	var master RpcMaster = RpcMaster{
		mutex:     new(sync.Mutex),
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

func (r *RpcMaster) RegisterRPC(name string, description string, incomingType string, returningType string,
	rpc func(*IOOperator) error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.registrar[r.counter] = &MasterCapabilities{
		Name:          name,
		Description:   description,
		IncomingType:  incomingType,
		ReturningType: returningType,
		rpc:           rpc,
	}
	r.counter++
}

func (r *RpcMaster) ShowCapabilities() ([]MasterCapabilitiesDTO, error) {

	var capabilities []MasterCapabilitiesDTO = make([]MasterCapabilitiesDTO, 0)

	for id, rpc := range r.registrar {
		capabilities = append(capabilities, MasterCapabilitiesDTO{
			RpcID:         id,
			Name:          rpc.Name,
			Description:   rpc.Description,
			IncomingType:  rpc.IncomingType,
			ReturningType: rpc.ReturningType,
		})
	}

	return capabilities, nil
}

func (r *RpcMaster) Start(ctx context.Context, ip net.IP, port int) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	socket, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   ip,
		Port: port,
	})
	if err != nil {
		return err
	}

	r.socket = socket

	for {
		select {

		case <-ctx.Done():
			return ctx.Err()

		default:
			tcpStream, err := socket.AcceptTCP()
			if err != nil {
				return err
			}

			go func() {
				defer tcpStream.Close()

				streamError := tcpStream.SetReadBuffer(BUFFER_SIZE)
				if streamError != nil {
					return
				}

				streamError = tcpStream.SetWriteBuffer(BUFFER_SIZE)
				if streamError != nil {
					return
				}

				streamError = tcpStream.SetKeepAlivePeriod(30 * time.Second)
				if streamError != nil {
					return
				}

				bufReader := bufio.NewReaderSize(tcpStream, BUFFER_SIZE)
				bufWriter := bufio.NewWriterSize(tcpStream, BUFFER_SIZE)

				for {
					headersBuffer := make([]byte, 12)
					_, streamError := io.ReadFull(bufReader, headersBuffer)
					if streamError != nil {
						return
					}

					ioStream := NewIOOperator(bufReader, bufWriter, binary.BigEndian.Uint64(headersBuffer[4:]))

					capability, ok := r.registrar[binary.BigEndian.Uint32(headersBuffer[:4])]
					if !ok {
						streamError = ioStream.WriteError("rpc not found")
						if streamError != nil {
							return
						}

						if ioStream.leftLength != 0 {
							streamError = discard(bufReader, r.discarder, ioStream.leftLength)
							if streamError != nil {
								return
							}
						}

						streamError = bufWriter.Flush()
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
						streamError = discard(bufReader, r.discarder, ioStream.leftLength)
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

					streamError = bufWriter.Flush()
					if streamError != nil {
						return
					}
				}
			}()
		}
	}
}

func (r *RpcMaster) Close() error {

	if r.socket == nil {
		return errors.New("invalid state for close operation")
	}

	return r.socket.Close()
}
