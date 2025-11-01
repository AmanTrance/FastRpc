package fastrpc

import (
	"errors"
	"io"
)

func readSpecifiedBytes(stream io.Reader, bytesCount int) ([]byte, error) {

	var bytesBuffer []byte = make([]byte, 0, bytesCount)
	for {
		var tempBuffer []byte = make([]byte, bytesCount)
		bytesRead, err := stream.Read(tempBuffer)
		if err != nil && err != io.EOF {
			return nil, err
		}

		bytesBuffer = append(bytesBuffer, tempBuffer[:bytesRead]...)

		if bytesRead < bytesCount && err != io.EOF {
			bytesCount -= bytesRead
		} else {
			if err == io.EOF {
				return bytesBuffer, io.EOF
			} else {
				break
			}
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
