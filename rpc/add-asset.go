package rpc

import (
	"errors"

	setlib "pendulev2/set2"

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
		return nil, errors.New("not found")
	}

	settingsCopy := set.Settings.Copy()
	settingsCopy.Assets = append(settingsCopy.Assets, r.Asset)

	if binancePair, _ := set.Settings.IsSupportedBinancePair(); binancePair {
		if b, _ := settingsCopy.IsSupportedBinancePair(); !b {
			return nil, errors.New("asset is not supported or incorrect")
		}
	} else {
		return nil, errors.New("not implemented")
	}

	defaultAsset, ok := setlib.DEFAULT_ASSETS[r.Asset.ID]
	if !ok {
		return nil, errors.New("Unknown asset: " + string(r.Asset.ID))
	}

	set.Assets[r.Asset.ID] = defaultAsset.Copy(set, r.Asset.MinDataDate, r.Asset.ID, r.Asset.Decimals)
	set.Settings = *settingsCopy
	return set.JSON()

}
