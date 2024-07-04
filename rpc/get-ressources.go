package rpc

import pcommon "github.com/pendulea/pendule-common"

func (s *RPCService) GetRessources(payload pcommon.RPCRequestPayload) (*pcommon.RessourcesJSON, error) {
	res := pcommon.BuildRessources()
	return &res, nil
}
