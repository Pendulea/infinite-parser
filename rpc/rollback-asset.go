package rpc

import (
	engine "pendulev2/task-engine"
	"pendulev2/util"

	pcommon "github.com/pendulea/pendule-common"
)

type RollBackAssetRequest struct {
	Address pcommon.AssetAddress `json:"address"`
	ToTime  int64                `json:"to_time"`
}

func (s *RPCService) RollbackAsset(payload pcommon.RPCRequestPayload) (interface{}, error) {
	r := RollBackAssetRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	parsed, err := r.Address.Parse()
	if err != nil {
		return nil, err
	}
	set := s.Sets.Find(parsed.IDString())
	if set == nil {
		return nil, util.ErrSetNotFound
	}

	asset := set.Assets[r.Address]
	if asset == nil {
		return nil, util.ErrAssetNotFound
	}

	date := pcommon.Format.FormatDateStr(pcommon.NewTimeUnit(r.ToTime).ToTime())

	return nil, engine.Engine.RollBackState(asset, date)
}
