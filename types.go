package fastrpc

type MasterCapabilities struct {
	Name          string `json:"name"`
	Description   string `json:"description"`
	IncomingType  string `json:"incomingType"`
	ReturningType string `json:"returningType"`
	rpc           func(*IOOperator) error
}

type MasterCapabilitiesDTO struct {
	RpcID         uint32 `json:"rpcId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	IncomingType  string `json:"incomingType"`
	ReturningType string `json:"returningType"`
}
