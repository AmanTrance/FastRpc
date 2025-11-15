package fastrpc

import (
	"io"
)

func discard(stream io.Reader, discarder io.Writer, length uint64) error {

	return readWriteSpecifiedBytes(stream, discarder, int(length), 0)
}
