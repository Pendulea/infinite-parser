package set2

import (
	"encoding/json"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
)

type assetReadlist struct {
	readList *map[time.Duration]read
	mu       sync.RWMutex
}

func newReadlistSet() *assetReadlist {
	return &assetReadlist{
		readList: nil,
		mu:       sync.RWMutex{},
	}
}

func (s *assetReadlist) isSet() bool {
	return s.readList != nil
}

type read struct {
	Time             pcommon.TimeUnit
	Timeframe        time.Duration
	prevState        *PrevState
	consistencyRange [2]pcommon.TimeUnit
}

func newRead(timeframe time.Duration, t0 pcommon.TimeUnit) *read {
	r := read{
		Time:      pcommon.NewTimeUnitFromTime(time.Now()),
		Timeframe: timeframe,
		consistencyRange: [2]pcommon.TimeUnit{
			t0,
			t0,
		},
		prevState: NewAssetPrevState(),
	}
	return &r
}

func (rl *assetReadlist) cacheAdd(timeframe time.Duration, t0 pcommon.TimeUnit) *read {
	r := newRead(timeframe, t0)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	_, exist := (*rl.readList)[r.Timeframe]
	if exist {
		return nil
	}
	(*rl.readList)[r.Timeframe] = *r
	return r
}

func (rl *assetReadlist) cacheReadTimeUpdate(timeframe time.Duration) *read {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	v, ok := (*rl.readList)[timeframe]
	if !ok {
		return nil
	}
	v.Time = pcommon.NewTimeUnitFromTime(time.Now())
	(*rl.readList)[timeframe] = v
	return &v
}

func (rl *assetReadlist) cachePrevStateUpdate(timeframe time.Duration, prevState *PrevState) *read {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	v, ok := (*rl.readList)[timeframe]
	if !ok {
		return nil
	}
	v.prevState = prevState
	(*rl.readList)[timeframe] = v
	return &v
}

func (rl *assetReadlist) cacheConsistencyUpdate(timeframe time.Duration, tMax pcommon.TimeUnit) *read {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	v, ok := (*rl.readList)[timeframe]
	if !ok {
		return nil
	}
	v.consistencyRange[1] = tMax
	(*rl.readList)[timeframe] = v
	return &v
}

func (rl *assetReadlist) remove(timeframe time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(*rl.readList, timeframe)
}

func (rl *assetReadlist) GetTimeFrameList() []time.Duration {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	var list []time.Duration
	for _, r := range *rl.readList {
		list = append(list, r.Timeframe)
	}
	return lo.Uniq(list)
}

func (rl *assetReadlist) GetConsistency(timeframe time.Duration) *[2]pcommon.TimeUnit {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	v, ok := (*rl.readList)[timeframe]
	if !ok {
		return nil
	}
	return &[2]pcommon.TimeUnit{v.consistencyRange[0], v.consistencyRange[1]}
}

func (rl *assetReadlist) GetPrevState(timeframe time.Duration) *PrevState {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	v, ok := (*rl.readList)[timeframe]
	if !ok {
		return nil
	}
	if v.prevState == nil {
		return nil
	}
	return v.prevState.Copy()
}

func (asset *AssetState) pullReadList() error {
	if asset.readList.isSet() {
		return nil
	}
	asset.readList.mu.Lock()
	defer asset.readList.mu.Unlock()

	txn := asset.SetRef.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(asset.GetReadListKey())
	if err != nil {
		if err == badger.ErrKeyNotFound {
			r := newRead(time.Second, asset.DataHistoryTime0())
			asset.readList.readList = &map[time.Duration]read{
				pcommon.Env.MIN_TIME_FRAME: *r,
			}
			return _storeReadList(asset)
		}
		return err
	}

	data, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}

	rl := map[time.Duration]read{}
	if err := json.Unmarshal(data, &rl); err != nil {
		return err
	}

	asset.readList.readList = &rl
	tmin := asset.DataHistoryTime0()
	for timeframe, r := range *asset.readList.readList {
		tmax, err := asset.pullLastConsistencyTimeFromDB(timeframe)
		if err != nil {
			return err
		}
		r.consistencyRange = [2]pcommon.TimeUnit{tmin, tmin}
		if tmax > tmin {
			r.consistencyRange[1] = tmax
		}
		r.prevState, err = asset.pullLastPrevStateFromDB(timeframe)
		if err != nil {
			return err
		}
		(*asset.readList.readList)[timeframe] = r
	}

	return nil
}

func _storeReadList(state *AssetState) error {
	txn := state.SetRef.db.NewTransaction(true)
	defer txn.Discard()

	list := state.readList.readList
	listBytes, err := json.Marshal(*list)
	if err != nil {
		return err
	}
	if err := txn.Set(state.GetReadListKey(), listBytes); err != nil {
		return err
	}

	return txn.Commit()
}

func (state *AssetState) onNewRead(timeframe time.Duration) error {
	if state.readList.cacheReadTimeUpdate(timeframe) != nil {
		return _storeReadList(state)
	}
	return nil
}

func (asset *AssetState) AddIfUnfoundInReadList(timeframe time.Duration) error {
	_, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}
	exist := asset.readList.cacheAdd(timeframe, asset.DataHistoryTime0())
	if exist != nil {
		return _storeReadList(asset)
	}
	return nil
}

func (state *AssetState) RemoveInReadList(timeframe time.Duration) error {
	state.readList.remove(timeframe)
	return _storeReadList(state)
}
