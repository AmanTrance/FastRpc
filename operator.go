package fastrpc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

type IOOperator struct {
	written    bool
	leftLength uint64
	mutex      *sync.Mutex
	stream     *net.TCPConn
}

func NewIOOperator(stream *net.TCPConn, readLength uint64) *IOOperator {
	return &IOOperator{
		written:    false,
		leftLength: readLength,
		mutex:      new(sync.Mutex),
		stream:     stream,
	}
}

func (i *IOOperator) ReadDataLeft() uint64 {
	return i.leftLength
}

func (i *IOOperator) ReadIOStream(count int) ([]byte, error) {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if uint64(count) > i.leftLength {
		return nil, errors.New("invalid count")
	}

	buf, err := readSpecifiedBytes(i.stream, count)
	if err != nil {
		return nil, err
	}

	i.leftLength -= uint64(count)

	return buf, nil
}

func (i *IOOperator) WriteIOFromBuffer(buf []byte) error {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.written {
		return nil
	} else {
		i.written = true
	}

	var metaDataBuffer []byte = make([]byte, 9)
	binary.BigEndian.PutUint64(metaDataBuffer[1:], uint64(len(buf)))
	err := writeSpecifiedBytes(i.stream, metaDataBuffer, 9)
	if err != nil {
		return err
	}

	return writeSpecifiedBytes(i.stream, buf, len(buf))
}

func (i *IOOperator) WriteIOFromReader(reader io.Reader, count int, chunkSize int) error {

	if chunkSize > count {
		return errors.New("chunkSize is not in bounds with count")
	}

	if chunkSize < 1 {
		chunkSize = count
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.written {
		return nil
	} else {
		i.written = true
	}

	var metaDataBuffer []byte = make([]byte, 9)
	binary.BigEndian.PutUint64(metaDataBuffer[1:], uint64(count))
	err := writeSpecifiedBytes(i.stream, metaDataBuffer, 9)
	if err != nil {
		return err
	}

	for count > 0 {
		if count >= chunkSize {
			buf, err := readSpecifiedBytes(reader, chunkSize)
			if err != nil {
				return err
			}

			err = writeSpecifiedBytes(i.stream, buf, chunkSize)
			if err != nil {
				return err
			}

			count -= chunkSize
		} else {
			buf, err := readSpecifiedBytes(reader, count)
			if err != nil {
				return err
			}

			err = writeSpecifiedBytes(i.stream, buf, count)
			if err != nil {
				return err
			}

			count = 0
		}
	}

	return nil
}

func (i *IOOperator) WriteNothing() error {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.written {
		return nil
	} else {
		i.written = true
	}

	err := writeSpecifiedBytes(i.stream, make([]byte, 9), 9)
	if err != nil {
		return err
	}

	return nil
}

func (i *IOOperator) WriteError(message string) error {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.written {
		return nil
	} else {
		i.written = true
	}

	var metaDataBuffer []byte = make([]byte, 9)
	metaDataBuffer[0] = 0b00000001
	binary.BigEndian.PutUint64(metaDataBuffer[1:], uint64(len(message)))
	err := writeSpecifiedBytes(i.stream, metaDataBuffer, 9)
	if err != nil {
		return err
	}

	return writeSpecifiedBytes(i.stream, []byte(message), len(message))
}
