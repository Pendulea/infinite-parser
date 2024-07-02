package rpc

import (
	engine "pendulev2/task-engine"
	"pendulev2/util"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

type AddTimeframeRequest struct {
	SetID     string `json:"set_id"`
	Timeframe int64  `json:"timeframe"` //timeframe in milliseconds
}

func (s *RPCService) AddTimeframe(payload pcommon.RPCRequestPayload) (interface{}, error) {
	r := AddTimeframeRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	timeframe := time.Duration(r.Timeframe) * time.Millisecond

	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return nil, err
	}

	set := s.Sets.Find(r.SetID)
	if set == nil {
		return nil, util.ErrSetNotFound
	}

	set.AddTimeframe(timeframe, engine.Engine.AddTimeframeIndexing)
	return nil, nil
}
