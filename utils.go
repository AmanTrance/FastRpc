package fastrpc

import (
	"errors"
	"io"
)

func readSpecifiedBytes(stream io.Reader, bytesCount int) ([]byte, error) {

	if bytesCount <= 0 {
		return nil, nil
	}

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

	if chunkSize > bytesCount {
		return errors.New("chunkSize is not in bounds with bytesCount")
	}

	if chunkSize < 1 {
		chunkSize = DEFAULT_CHUNK_SIZE
	}

	for bytesCount > 0 {
		if bytesCount >= chunkSize {
			buf, err := readSpecifiedBytes(readStream, chunkSize)
			if err != nil {
				return err
			}

			err = writeSpecifiedBytes(writeStream, buf, chunkSize)
			if err != nil {
				return err
			}

			bytesCount -= chunkSize
		} else {
			buf, err := readSpecifiedBytes(readStream, bytesCount)
			if err != nil {
				return err
			}

			err = writeSpecifiedBytes(writeStream, buf, bytesCount)
			if err != nil {
				return err
			}

			bytesCount = 0
		}
	}

	return nil
}
