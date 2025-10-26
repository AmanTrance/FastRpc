package fastrpc

type MasterCapabilities struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	IncomingEncoding  string `json:"incomingEncoding"`
	ReturningEncoding string `json:"returningEncoding"`
	rpc               func(*IOOperator) error
}

type MasterCapabilitiesDTO struct {
	RpcID             uint32 `json:"rpcId"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	IncomingEncoding  string `json:"incomingEncoding"`
	ReturningEncoding string `json:"returningEncoding"`
}
