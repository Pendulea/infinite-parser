package rpc

import (
	"fmt"
	manager "pendulev2/set-manager"
	"pendulev2/set2"
	"pendulev2/util"
	"strings"

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

	list, err := manager.PullListFromJSON(manager.GetJSONPath())
	if err != nil {
		return nil, err
	}

	if r.Asset.MinDataDate == "" {
		if len(r.Asset.Address.Dependencies) == 0 {
			return nil, fmt.Errorf("min_data_date is required")
		} else {
			dependencies := r.Asset.Address.Dependencies
			list := []*set2.AssetState{}
			for _, dep := range dependencies {
				parsedAddress, err := dep.Parse()
				if err != nil {
					return nil, err
				}
				s := s.Sets.Find(parsedAddress.IDString())
				if s == nil {
					return nil, fmt.Errorf("dependency not found")
				}
				asset := s.Assets[dep]
				if asset == nil {
					return nil, fmt.Errorf("dependency not found")
				}
				list = append(list, asset)
			}

			if len(list) != len(dependencies) {
				return nil, fmt.Errorf("one or more dependencies not found")
			}

			min := "2099-12-31"
			for _, dep := range list {
				settings := dep.Settings()
				if settings.MinDataDate == "" {
					return nil, fmt.Errorf("dependency min_data_date is required")
				}
				if strings.Compare(min, settings.MinDataDate) > 0 {
					min = settings.MinDataDate
				}
			}
			if min == "2099-12-31" {
				return nil, fmt.Errorf("no min date found")
			}
			r.Asset.MinDataDate = min
		}
	}

	if err := set.AddAsset(r.Asset); err != nil {
		return nil, err
	}

	json, err := set.JSON()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(list); i++ {
		if list[i].IDString() == r.SetID {
			list[i] = set.Settings
			break
		}
	}

	if err := manager.UpdateListToJSON(list); err != nil {
		return nil, err
	}

	return json, nil
}
