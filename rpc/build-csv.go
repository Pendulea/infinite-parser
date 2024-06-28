package rpc

import (
	engine "pendulev2/task-engine"

	pcommon "github.com/pendulea/pendule-common"
)

func (s *RPCService) BuildCSV(payload pcommon.RPCRequestPayload) (interface{}, error) {
	r := engine.CSVBuildingOrderPacked{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	return nil, engine.Engine.AddCSVBuilding(r)
}
