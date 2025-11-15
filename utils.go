package fastrpc

import (
	"errors"
	"io"
)

func readSpecifiedBytes(stream io.Reader, bytesCount int) ([]byte, error) {

	if bytesCount <= 0 {
		return nil, nil
	}

	bytesBuffer := make([]byte, bytesCount)
	_, err := io.ReadFull(stream, bytesBuffer)
	if err != nil {
		return nil, err
	}

	return bytesBuffer, nil
}

func writeSpecifiedBytes(stream io.Writer, buf []byte, bytesCount int) error {

	if bytesCount <= 0 {
		return nil
	}

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

func readWriteSpecifiedBytes(readStream io.Reader, writeStream io.Writer, bytesCount int, chunkSize int) error {

	_, err := io.CopyN(writeStream, readStream, int64(bytesCount))
	return err
}
