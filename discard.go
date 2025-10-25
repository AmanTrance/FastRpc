package fastrpc

import (
	"io"
	"net"
)

func discard(stream *net.TCPConn, discarder io.Writer, length uint64) error {

	buf, err := readSpecifiedBytes(stream, int(length))
	if err != nil {
		return err
	}

	err = writeSpecifiedBytes(discarder, buf, int(length))
	if err != nil {
		return err
	}

	return nil
}
