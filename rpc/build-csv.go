package rpc

import (
	engine "pendulev2/task-engine"

	pcommon "github.com/pendulea/pendule-common"
)

type BuildCSVRequest struct {
	Orders    [][]string `json:"orders"`
	Timeframe int64      `json:"timeframe"` //in milliseconds
	From      int64      `json:"from"`
	To        int64      `json:"to"`
}

func (s *RPCService) BuildCSV(payload pcommon.RPCRequestPayload) (interface{}, error) {
	r := BuildCSVRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}
	return nil, engine.Engine.AddCSVBuilding(r.From, r.To, r.Timeframe, r.Orders)
}
