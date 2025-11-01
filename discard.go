package fastrpc

import (
	"io"
	"net"
)

func discard(stream *net.TCPConn, discarder io.Writer, length uint64) error {

	return readWriteSpecifiedBytes(stream, discarder, int(length), DEFAULT_CHUNK_SIZE)
}
