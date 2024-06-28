package set2

import (
	"fmt"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

type AssetState struct {
	key                        [2]byte
	id                         pcommon.AssetType
	t                          pcommon.DataType
	consistencyMaxLookbackDays int
	precision                  int8             //precision of the data
	readList                   *assetReadlist   //timeframe list and last read
	start                      pcommon.TimeUnit //data start time

	SetRef *Set //reference to the set
}

func (state *AssetState) SetAndAssetID() string {
	return state.SetRef.ID() + ":" + string(state.ID())
}

func (state *AssetState) Precision() int8 {
	return state.precision
}

func (state *AssetState) Key() [2]byte {
	return state.key
}

func (state *AssetState) ID() pcommon.AssetType {
	return state.id
}

func (state *AssetState) Type() pcommon.DataType {
	return state.t
}

func (state *AssetState) DataT0() pcommon.TimeUnit {
	return state.start
}

func (state *AssetState) JSON(timeframe time.Duration) (*pcommon.AssetJSON, error) {
	t, err := state.GetLastConsistencyTime(timeframe)
	if err != nil {
		return nil, err
	}
	consistency := [2]pcommon.TimeUnit{state.start, state.start}
	if t > state.start {
		consistency[1] = t
	}

	ret := pcommon.AssetJSON{
		ID:                         state.id,
		Precision:                  state.precision,
		Type:                       state.t,
		ConsistencyMaxLookbackDays: state.consistencyMaxLookbackDays,
		ConsistencyRange:           consistency,
		Timeframe:                  timeframe.Milliseconds(),
		SubAssets:                  nil,
	}
	if (state.IsQuantity() || state.IsUnit()) && timeframe == pcommon.Env.MIN_TIME_FRAME {
		ret.SubAssets = make([]pcommon.AssetJSON, 0)
		for _, v := range *state.readList.readList {
			j, err := state.JSON(v.Timeframe)
			if err != nil {
				return nil, err
			}
			ret.SubAssets = append(ret.SubAssets, *j)
		}
	}

	return &ret, nil
}

func (state *AssetState) Copy(SetRef *Set, minDataDate string, id pcommon.AssetType, precision int8) *AssetState {
	t, err := pcommon.Format.StrDateToDate(minDataDate)
	if err != nil {
		log.Fatal(err)
	}
	start := pcommon.NewTimeUnitFromTime(t)

	newState := AssetState{
		key:                        state.key,
		t:                          state.t,
		start:                      start,
		id:                         id,
		consistencyMaxLookbackDays: state.consistencyMaxLookbackDays,
		SetRef:                     SetRef,
		readList:                   newReadlistSet(),
		precision:                  precision,
	}

	if err := newState.pullReadList(); err != nil {
		log.Fatal(err)
	}
	return &newState
}

func newUninitalizedAssetState(key [2]byte, dataType pcommon.DataType, consistencyMaxLookbackDays int) AssetState {
	return AssetState{
		key:                        key,
		t:                          dataType,
		SetRef:                     nil,
		readList:                   newReadlistSet(),
		precision:                  -1,
		consistencyMaxLookbackDays: consistencyMaxLookbackDays,
		start:                      0,
		id:                         "",
	}
}

func (state *AssetState) IsUnit() bool {
	return state.t == pcommon.UNIT
}

func (state *AssetState) IsQuantity() bool {
	return state.t == pcommon.QUANTITY
}

func (state *AssetState) IsPoint() bool {
	return state.t == pcommon.POINT
}

func (state *AssetState) PrintReadList() {
	fmt.Printf("Readlist: %s of %s\n", state.ID(), state.SetRef.ID())
	for _, v := range *state.readList.readList {
		fmt.Println(v.Timeframe, v.Time.ToTime())
	}
	fmt.Println()
	fmt.Println()
}
