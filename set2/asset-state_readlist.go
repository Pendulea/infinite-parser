package set2

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

type assetReadlist struct {
	readList *map[string]read
	mu       sync.RWMutex
	building bool
}

func newReadlistSet() *assetReadlist {
	return &assetReadlist{
		readList: nil,
		mu:       sync.RWMutex{},
		building: false,
	}
}

func (s *assetReadlist) isBuilding() bool {
	return s.building
}

func (s *assetReadlist) isSet() bool {
	return s.readList != nil
}

type read struct {
	Time      pcommon.TimeUnit
	Timeframe time.Duration
}

func (r *read) key() string {
	return strconv.FormatInt(int64(r.Timeframe), 10)
}

func newRead(timeframe time.Duration) *read {
	r := read{
		Time:      pcommon.NewTimeUnitFromTime(time.Now()),
		Timeframe: timeframe,
	}
	return &r
}

func (rl *assetReadlist) add(timeframe time.Duration) *read {
	r := newRead(timeframe)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	_, exist := (*rl.readList)[r.key()]
	if exist {
		return nil
	}
	(*rl.readList)[r.key()] = *r
	return r
}

func (rl *assetReadlist) update(timeframe time.Duration) *read {
	r := newRead(timeframe)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	(*rl.readList)[r.key()] = *r
	return r
}

func (rl *assetReadlist) remove(timeframe time.Duration) {
	r := newRead(timeframe)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(*rl.readList, r.key())
}

func (rl *assetReadlist) GetTimeFrameList() []time.Duration {
	var list []time.Duration
	for _, r := range *rl.readList {
		list = append(list, r.Timeframe)
	}
	return pcommon.Unique(list)
}

func (state *AssetState) pullReadList() error {
	if state.readList.isSet() {
		return nil
	}

	for state.readList.isBuilding() {
		time.Sleep(time.Millisecond * 300)
		return nil
	}
	state.readList.building = true
	state.readList.mu.Lock()
	defer state.readList.mu.Unlock()

	txn := state.SetRef.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(state.GetReadListKey())
	if err != nil {
		if err == badger.ErrKeyNotFound {
			state.readList.readList = &map[string]read{}
			return nil
		}
		return err
	}

	data, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}

	rl := map[string]read{}
	if err := json.Unmarshal(data, &rl); err != nil {
		return err
	}

	state.readList.readList = &rl
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

func (state *AssetState) updateInReadList(timeframe time.Duration) error {
	state.readList.update(timeframe)
	return _storeReadList(state)
}

func (state *AssetState) AddIfUnfoundInReadList(timeframe time.Duration) error {
	_, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}
	exist := state.readList.add(timeframe)
	if exist != nil {
		return _storeReadList(state)
	}
	return nil
}

func (state *AssetState) RemoveInReadList(timeframe time.Duration) error {
	state.readList.remove(timeframe)
	return _storeReadList(state)
}
