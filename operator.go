package fastrpc

import (
	"encoding/binary"
	"net"
)

type IOOperator struct {
	stream *net.TCPConn
}

func (i *IOOperator) Read(buf []byte) (int, error) {

	return i.stream.Read(buf)
}

func (i *IOOperator) WriteBuffer(buf []byte) error {

	var lengthBuffer []byte = make([]byte, 8)
	binary.LittleEndian.PutUint64(lengthBuffer, uint64(len(buf)))
	err := writeSpecifiedBytes(i.stream, lengthBuffer, 8)
	if err != nil {
		return err
	}

	return writeSpecifiedBytes(i.stream, buf, len(buf))
}

func writeSpecifiedBytes(stream *net.TCPConn, buf []byte, bytesCount int) error {

	for {
		bytesWrite, err := stream.Write(buf[:bytesCount])
		if err != nil {
			return err
		}

		if bytesWrite != bytesCount {
			buf = buf[bytesWrite:]
			bytesCount -= bytesWrite
		} else {
			break
		}
	}

	return nil
}
