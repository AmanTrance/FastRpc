package fastrpc

import (
	"errors"
	"io"
)

func readSpecifiedBytes(stream io.Reader, bytesCount int) ([]byte, error) {

	var bytesBuffer []byte = make([]byte, bytesCount)
	for {
		var tempBuffer []byte = make([]byte, bytesCount)
		bytesRead, err := stream.Read(bytesBuffer)
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

func writeSpecifiedBytes(stream io.Writer, buf []byte, bytesCount int) error {

	if bytesCount > len(buf) {
		return errors.New("bytesCount is greater than len(buf)")
	}

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
