package rpc

import (
	"pendulev2/util"

	pcommon "github.com/pendulea/pendule-common"
)

type AddAssetRequest struct {
	SetID string                `json:"set_id"`
	Asset pcommon.AssetSettings `json:"asset"`
}

// CheckCandlesExist checks if candles exist for the given date and time frame.
func (s *RPCService) AddAsset(payload pcommon.RPCRequestPayload) (*pcommon.SetJSON, error) {
	r := AddAssetRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	set := s.Sets.Find(r.SetID)
	if set == nil {
		return nil, util.ErrSetNotFound
	}

	if err := set.AddAsset(r.Asset); err != nil {
		return nil, err
	}

	return set.JSON()
}
