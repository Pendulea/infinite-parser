package rpc

import (
	"fmt"
	"pendulev2/util"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

type GetTicksRequest struct {
	Timeframe int64                `json:"timeframe"` //In milliseconds
	Address   pcommon.AssetAddress `json:"address"`
	FromTime  int64                `json:"from_time"`
	ToTime    int64                `json:"to_time"`
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

	from := pcommon.NewTimeUnit(r.FromTime)
	to := pcommon.NewTimeUnit(r.ToTime)

	consistencyTime, err := asset.GetLastConsistencyTimeCached(timeframe)
	if err != nil {
		return nil, err
	}

	// If the consistency time is the same as the end time, we need to add a second to the to time to get the last tick
	if consistencyTime == to {
		to = to.Add(time.Second)
	}

	list, err := asset.GetInDataRange(from, to, timeframe, nil, nil, true)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Get %d Ticks %s : +%s\n", list.Len(), asset.Address(), time.Since(start).String())
	return &TickList{
		List:     list,
		DataType: asset.DataType(),
	}, nil
}
