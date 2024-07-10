package rpc

import (
	"fmt"
	setlib "pendulev2/set2"
	"pendulev2/util"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

type GetTicksRequest struct {
	Timeframe      int64                `json:"timeframe"` //In milliseconds
	Address        pcommon.AssetAddress `json:"address"`
	OffsetUnixTime int64                `json:"offset_unix_time"`
}

type TickList struct {
	List     pcommon.DataList `json:"list"`
	DataType pcommon.DataType `json:"data_type"`
}

func (s *RPCService) GetTicks(payload pcommon.RPCRequestPayload) (*TickList, error) {
	start := time.Now()
	r := GetTicksRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}

	timeframe := time.Duration(r.Timeframe) * time.Millisecond
	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
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
	settings := setlib.DataLimitSettings{
		TimeFrame:      timeframe,
		Limit:          1000,
		OffsetUnixTime: pcommon.NewTimeUnit(r.OffsetUnixTime),
		StartByEnd:     true,
	}

	list, err := asset.GetDataLimit(settings, true)
	if err != nil {
		return nil, err
	}

	fmt.Println("GetTicks took", time.Since(start))
	return &TickList{
		List:     list,
		DataType: asset.DataType(),
	}, nil
}
