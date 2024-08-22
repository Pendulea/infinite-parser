package set2

import (
	"bytes"
	"errors"
	"math"
	"pendulev2/util"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

type PrevState struct {
	state   []byte
	min     float64
	minTime pcommon.TimeUnit
	max     float64
	maxTime pcommon.TimeUnit
}

func NewAssetPrevState() *PrevState {
	return &PrevState{
		state:   []byte{},
		min:     math.MaxFloat64,
		max:     -math.MaxFloat64,
		minTime: pcommon.NewTimeUnit(0),
		maxTime: pcommon.NewTimeUnit(0),
	}
}

func (ps *PrevState) Copy() *PrevState {
	return &PrevState{
		state:   append([]byte{}, ps.state...),
		min:     ps.min,
		max:     ps.max,
		minTime: ps.minTime,
		maxTime: ps.maxTime,
	}
}

func (ps *PrevState) CheckUpdateMin(min float64, minTime pcommon.TimeUnit) {
	if min < ps.min {
		ps.min = min
		ps.minTime = minTime
	}
}

func (ps *PrevState) CheckUpdateMax(max float64, maxTime pcommon.TimeUnit) {
	if max > ps.max {
		ps.max = max
		ps.maxTime = maxTime
	}
}

func (ps *PrevState) UpdateState(state []byte) {
	ps.state = append([]byte{}, state...)
}

func (ps *PrevState) State() []byte {
	return append([]byte{}, ps.state...)
}

func (ps *PrevState) Compact() []byte {
	minBytes := pcommon.Bytes.Int64ToBytes(ps.minTime.Int())
	maxBytes := pcommon.Bytes.Int64ToBytes(ps.maxTime.Int())
	return append(append(minBytes, maxBytes...), ps.state...)
}

func (ps *PrevState) IsEqual(other *PrevState) bool {
	return bytes.Equal(ps.state, other.state) && ps.min == other.min && ps.max == other.max && ps.minTime == other.minTime && ps.maxTime == other.maxTime
}

func (ps *PrevState) IsEmpty() bool {
	return ps.IsEqual(NewAssetPrevState())
}

func (asset *AssetState) pullLastPrevStateFromDB(timeframe time.Duration) (*PrevState, error) {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return nil, err
	}

	prevState, err := asset.searchForLastPrevStateFromDB(timeframe)
	if err != nil {
		return nil, err
	}
	if prevState == nil {
		return NewAssetPrevState(), nil
	}

	minTimeBytes := prevState[:8]
	minTimeInt := pcommon.NewTimeUnit(util.BytesToInt64(minTimeBytes))
	maxTimeBytes := prevState[8:16]
	maxTimeInt := pcommon.NewTimeUnit(util.BytesToInt64(maxTimeBytes))
	prevState = prevState[16:]

	txn := asset.NewTX(false)
	defer txn.Discard()
	itmMin, err := txn.Get(asset.GetDataKey(label, minTimeInt))
	if err != nil {
		return nil, err
	}
	minData, err := itmMin.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	itmMax, err := txn.Get(asset.GetDataKey(label, maxTimeInt))
	if err != nil {
		return nil, err
	}
	maxData, err := itmMax.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	minUnraw, err := pcommon.ParseTypeData(asset.DataType(), minData, minTimeInt)
	if err != nil {
		return nil, err
	}
	maxUnraw, err := pcommon.ParseTypeData(asset.DataType(), maxData, minTimeInt)
	if err != nil {
		return nil, err
	}
	ps := &PrevState{
		state:   prevState,
		min:     minUnraw.Min(),
		max:     maxUnraw.Max(),
		minTime: minTimeInt,
		maxTime: maxTimeInt,
	}
	return ps, nil
}

func (asset *AssetState) searchForLastPrevStateFromDB(timeframe time.Duration) ([]byte, error) {
	tMax, err := asset.pullLastConsistencyTimeFromDB(timeframe)
	if err != nil {
		return nil, err
	}
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return nil, err
	}

	limit := pcommon.Format.FormatDateStr(asset.DataHistoryTime0().ToTime())
	var dayBack time.Duration = 0
	txn := asset.NewTX(false)
	defer txn.Discard()
	for {
		date := pcommon.Format.FormatDateStr(tMax.Add((time.Hour * 24) * -dayBack).ToTime())
		key := asset.GetPrevStateKey(label, date)
		itm, err := txn.Get(key)
		if err == nil {
			return itm.ValueCopy(nil)
		}
		if err != badger.ErrKeyNotFound {
			return nil, err
		}
		if date == limit {
			break
		}
		dayBack++
	}

	return nil, nil
}

func (asset *AssetState) GetLastPrevStateCached(timeframe time.Duration) (*PrevState, error) {
	cached := asset.readList.GetPrevState(timeframe)
	if cached == nil {
		return nil, errors.New("no cached prev state")
	}
	return cached.Copy(), nil
}

func (asset *AssetState) storePrevState(newPrevState *PrevState, timeframe time.Duration, stateTime pcommon.TimeUnit) error {
	if newPrevState.IsEmpty() {
		return nil
	}
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	date := pcommon.Format.FormatDateStr(stateTime.ToTime())

	txn := asset.NewTX(true)
	defer txn.Discard()

	key := asset.GetPrevStateKey(label, date)
	data := newPrevState.Compact()
	if err := txn.Set(key, data); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}

	asset.readList.strictPrevStateUpdate(timeframe, newPrevState.Copy())
	return nil
}

/*
RollbackPrevState deletes all prev states from the asset state until the given date.
toDate: the date to rollback to formatted as "YYYY-MM-DD" (The previous day at 23:59:59 will be the new consistency time)
*/
func (state *AssetState) rollbackPrevState(toDate string, timeframe time.Duration) error {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	txn := state.SetRef.db.NewTransaction(true)
	defer txn.Discard()

	t0, err := pcommon.Format.StrDateToDate(toDate)
	if err != nil {
		return err
	}

	for t0.UnixNano() < time.Now().UnixNano() {
		key := state.GetPrevStateKey(label, pcommon.Format.FormatDateStr(t0))
		if err := txn.Delete(key); err != nil {
			return err
		}
		t0 = t0.Add(time.Hour * 24)
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	lastState, err := state.pullLastPrevStateFromDB(timeframe)
	if err != nil {
		return err
	}

	state.readList.strictPrevStateUpdate(timeframe, lastState)
	return nil
}
