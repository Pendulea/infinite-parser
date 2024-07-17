package rpc

import (
	"strings"

	pcommon "github.com/pendulea/pendule-common"
)

type AddSetRequest struct {
	Symbol string `json:"symbol"`
}

func (s *RPCService) AddSet(payload pcommon.RPCRequestPayload) (*pcommon.SetJSON, error) {
	r := AddSetRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	setSettings := pcommon.SetSettings{
		Assets: []pcommon.AssetSettings{},
		ID:     []string{strings.ToUpper(r.Symbol), "USDT"},
		Settings: map[string]int64{
			"binance": 1,
		},
	}

	err = s.SM.Add(setSettings, true)
	if err != nil {
		return nil, err
	}

	set := s.Sets.Find(setSettings.IDString())
	return set.JSON()
}
