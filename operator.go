package fastrpc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	DEFAULT_CHUNK_SIZE = 65536
)

type IOOperator struct {
	written    bool
	leftLength uint64
	mutex      *sync.Mutex
	reader     *bufio.Reader
	writer     *bufio.Writer
}

func NewIOOperator(reader *bufio.Reader, writer *bufio.Writer, readLength uint64) *IOOperator {
	return &IOOperator{
		written:    false,
		leftLength: readLength,
		mutex:      new(sync.Mutex),
		reader:     reader,
		writer:     writer,
	}
}

func (i *IOOperator) ReadDataLeft() uint64 {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.leftLength
}

func (i *IOOperator) ReadIOStream(count int) ([]byte, error) {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if uint64(count) > i.leftLength {
		return nil, errors.New("invalid count")
	}

	buf, err := readSpecifiedBytes(i.reader, count)
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
	err := writeSpecifiedBytes(i.writer, metaDataBuffer, 9)
	if err != nil {
		return err
	}

	return writeSpecifiedBytes(i.writer, buf, len(buf))
}

func (i *IOOperator) WriteIOFromReader(reader io.Reader, count int, chunkSize int) error {

	if chunkSize > count {
		return errors.New("chunkSize is not in bounds with count")
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
	err := writeSpecifiedBytes(i.writer, metaDataBuffer, 9)
	if err != nil {
		return err
	}

	return readWriteSpecifiedBytes(reader, i.writer, count, chunkSize)
}

func (i *IOOperator) WriteNothing() error {

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.written {
		return nil
	} else {
		i.written = true
	}

	err := writeSpecifiedBytes(i.writer, make([]byte, 9), 9)
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
	err := writeSpecifiedBytes(i.writer, metaDataBuffer, 9)
	if err != nil {
		return err
	}

	return writeSpecifiedBytes(i.writer, []byte(message), len(message))
}
