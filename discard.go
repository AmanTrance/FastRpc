package fastrpc

import (
	"errors"
	"net"
)

func (r *RpcMaster) Discard(stream *net.TCPConn, length int64) error {

	actualDiscardLength, err := stream.WriteTo(r.discarder)
	if err != nil {
		return err
	}

	if actualDiscardLength != length {
		return errors.New("[protocol violation]: unexpected length")
	}

	return nil
}
