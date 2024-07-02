package rpc

import (
	"time"

	setlib "pendulev2/set2"

	pcommon "github.com/pendulea/pendule-common"
)

type GetTicksRequest struct {
	To             int64      `json:"to"`        //In unix seconds
	Timeframe      int64      `json:"timeframe"` //In milliseconds
	Orders         [][]string `json:"orders"`    // [Address, columns...]
	OffsetUnixTime int64      `json:"offset_unix_time"`
}

type TickList struct {
	List     interface{}      `json:"list"`
	DataType pcommon.DataType `json:"data_type"`
}

type GetTicksResponse struct {
	Assets map[pcommon.AssetAddress]TickList `json:"assets"`
}

func (s *RPCService) GetTicks(payload pcommon.RPCRequestPayload) (*GetTicksResponse, error) {
	r := GetTicksRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	timeframe := time.Duration(r.Timeframe) * time.Millisecond
	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return nil, err
	}

	listOrders, err := setlib.ParseArrayOrder(*s.Sets, timeframe, r.Orders)
	if err != nil {
		return nil, err
	}
	ret := &GetTicksResponse{
		Assets: make(map[pcommon.AssetAddress]TickList),
	}
	for _, order := range listOrders {
		settings := setlib.DataLimitSettings{
			TimeFrame:      timeframe,
			Limit:          2000,
			OffsetUnixTime: pcommon.NewTimeUnit(r.OffsetUnixTime),
			StartByEnd:     true,
		}
		ticks, err := order.Asset.GetDataLimit(settings, true)
		if err != nil {
			return nil, err
		}

		address := order.Asset.Address()
		tl := TickList{
			DataType: order.Asset.DataType(),
		}
		tl.List, err = ticks.ToJSON(order.Columns.Columns())
		if err != nil {
			return nil, err
		}
		ret.Assets[address] = tl
	}
	return ret, nil
}
