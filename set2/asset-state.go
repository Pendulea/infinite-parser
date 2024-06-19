package set2

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

type AssetStates []*AssetState

func (states AssetStates) LeastConsistent(timeframe time.Duration) (*AssetState, error) {
	leastConsistentTime := pcommon.NewTimeUnitFromTime(time.Now())
	var ret *AssetState = nil

	for _, state := range states {
		t, err := state.IsConsistentUntil(timeframe)
		if err != nil {
			return nil, err
		}
		if t < leastConsistentTime {
			leastConsistentTime = t
			ret = state
		}
	}

	return ret, nil
}

type AssetState struct {
	key       [2]byte
	id        string
	t         DataType
	precision int8
	readList  *assetReadlist   //timeframe list and last read
	start     pcommon.TimeUnit //data start time

	SetRef *Set //reference to the set
}

func (state *AssetState) Precision() int8 {
	return state.precision
}

func (state *AssetState) Key() [2]byte {
	return state.key
}

func (state *AssetState) ID() string {
	return state.id
}

func (state *AssetState) Type() DataType {
	return state.t
}

func (state *AssetState) DataT0() pcommon.TimeUnit {
	return state.start
}

func (state *AssetState) ReindexInterval(t0 pcommon.TimeUnit, t1 pcommon.TimeUnit) {

}

func (state *AssetState) GetActiveTimeFrameList() []time.Duration {
	return state.readList.GetTimeFrameList()
}

func (state *AssetState) IsTimeframeSupported(timeframe time.Duration) bool {
	if timeframe == pcommon.Env.MIN_TIME_FRAME {
		return true
	}
	list := state.GetActiveTimeFrameList()
	for _, tf := range list {
		if tf == timeframe {
			return true
		}
	}
	return false
}

func (state *AssetState) IsTimeframeIndexUpToDate(timeFrame time.Duration) (bool, error) {
	l1, err := state.GetLastTimeframeIndexingDate(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return false, err
	}

	l2, err := state.GetLastTimeframeIndexingDate(timeFrame)
	if err != nil {
		return false, err
	}
	if l2 == 0 {
		return false, nil
	}

	tTF := l2.Add(timeFrame)
	return pcommon.Format.FormatDateStr(l1.ToTime()) == pcommon.Format.FormatDateStr(l2.ToTime()) || !(tTF < l1), nil
}

func (state *AssetState) GetLastTimeframeIndexingDate(timeFrame time.Duration) (pcommon.TimeUnit, error) {
	t, l1, err := state.GetLatestData(timeFrame)
	if err != nil {
		return 0, err
	}
	if t == nil {
		return 0, nil
	}

	return l1, nil
}

func (state *AssetState) GetTimeFrameToReindex() ([]time.Duration, error) {
	c, err := state.IsConsistent(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return nil, err
	}
	if !c {
		return nil, nil
	}

	var reindex []time.Duration
	for _, tf := range state.GetActiveTimeFrameList() {
		sync, err := state.IsTimeframeIndexUpToDate(tf)
		if err != nil {
			return nil, err
		}
		if !sync {
			reindex = append(reindex, tf)
		}
	}

	return reindex, nil
}

func (state *AssetState) SetStart(date string) {
	t, err := pcommon.Format.StrDateToDate(date)
	if err != nil {
		log.Fatal(err)
	}
	state.start = pcommon.NewTimeUnitFromTime(t)
}

func (state *AssetState) Copy(SetRef *Set, id string, precision int8) *AssetState {
	newState := AssetState{
		key:       state.key,
		t:         state.t,
		start:     state.start,
		id:        id,
		SetRef:    SetRef,
		readList:  newReadlistSet(),
		precision: precision,
	}

	if err := newState.pullReadList(); err != nil {
		log.Fatal(err)
	}
	return &newState
}

func (state *AssetState) Sync() (*string, error) {
	t, err := state.GetLastStoreBatchTime(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return nil, err
	}

	if t == 0 {
		s := pcommon.Format.FormatDateStr(state.DataT0().ToTime())
		return &s, nil
	}

	offset := time.Duration(pcommon.Env.MAX_DAYS_BACKWARD_FOR_CONSISTENCY-1) * 24 * time.Hour

	max := pcommon.NewTimeUnitFromTime(time.Now()).Add(-offset)
	if t < max {
		s := pcommon.Format.FormatDateStr(t.ToTime())
		return &s, nil
	}
	return nil, nil
}

func newUninitalizedAssetState(key [2]byte, dataType DataType) AssetState {
	return AssetState{
		key:      key,
		t:        dataType,
		SetRef:   nil,
		readList: newReadlistSet(),
	}
}

func (state *AssetState) IsUnit() bool {
	return state.t == UNIT
}

func (state *AssetState) IsQuantity() bool {
	return state.t == QUANTITY
}

func (state *AssetState) IsPoint() bool {
	return state.t == POINT
}

func (state *AssetState) IsConsistent(timeframe time.Duration) (bool, error) {
	t, err := state.GetLastStoreBatchTime(timeframe)
	if err != nil {
		return false, err
	}
	minEndAllowed, _ := pcommon.Format.StrDateToDate(pcommon.Format.BuildDateStr(pcommon.Env.MAX_DAYS_BACKWARD_FOR_CONSISTENCY - 1))
	return pcommon.NewTimeUnitFromTime(minEndAllowed) < t, nil
}

func (state *AssetState) IsConsistentUntil(timeframe time.Duration) (pcommon.TimeUnit, error) {
	return state.GetLastStoreBatchTime(timeframe)
}

func (state *AssetState) SetNewLastDataTime(timeframe time.Duration, newLastDataTime pcommon.TimeUnit, tx *badger.Txn) error {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	txWasNil := false
	if tx == nil {
		txWasNil = true
		tx = state.SetRef.db.NewTransaction(true)

	}

	if err := tx.Set(state.GetLastDataTimeKey(label), []byte(newLastDataTime.String())); err != nil {
		return err
	}

	if txWasNil {
		return tx.Commit()
	}
	return nil

}

func (state *AssetState) Store(data map[pcommon.TimeUnit][]byte, timeframe time.Duration, newLastDataTime pcommon.TimeUnit) error {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	if len(data) > 0 {
		BATCH_SIZE := 10_000 // Define the maximum number of entries per transaction
		i := 0
		txn := state.SetRef.db.NewTransaction(true)
		for dataTime, serial := range data {
			if i == BATCH_SIZE {
				// Commit the transaction for the current batch
				if err := txn.Commit(); err != nil {
					return err
				}
				txn = state.SetRef.db.NewTransaction(true)
				i = 0
			}

			storedKey := state.GetDataKey(label, dataTime)
			if err := txn.Set(storedKey, serial); err != nil {
				txn.Discard()
				return err
			}
			i++
		}
		if i > 0 {
			if err := txn.Commit(); err != nil {
				return err
			}
		}
	}

	if newLastDataTime > 0 {
		if err := state.SetNewLastDataTime(timeframe, newLastDataTime, nil); err != nil {
			return err
		}
	}

	return nil
}

func (state *AssetState) Delete(timeFrame time.Duration, updateLastDeletedElemDate func(pcommon.TimeUnit, int)) (int, error) {
	if timeFrame == pcommon.Env.MIN_TIME_FRAME {
		return 0, errors.New("cannot delete MIN_TIME_FRAME date")
	}

	label, err := pcommon.Format.TimeFrameToLabel(timeFrame)
	if err != nil {
		return 0, err
	}

	t0 := state.DataT0()

	// Open a BadgerDB transaction
	txn := state.SetRef.db.NewTransaction(true)
	defer txn.Discard()

	// Creating a key range for deletion
	startKey := state.GetDataKey(label, t0)
	limitKey := state.GetDataKey(label, pcommon.NewTimeUnitFromTime(time.Now()))

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	totalDeleted := 0

	// Batch size for deletion
	const BATCH_SIZE = 10_000

	for {
		txn := state.SetRef.db.NewTransaction(true)
		defer txn.Discard()
		iter := txn.NewIterator(opts)
		defer iter.Close()

		elemDeletedInBatch := 0
		var lastKey []byte

		for iter.Seek(startKey); iter.Valid(); iter.Next() {
			key := iter.Item().KeyCopy(nil)
			lastKey = key
			if elemDeletedInBatch >= BATCH_SIZE {
				break
			}
			if bytes.Compare(key, limitKey) >= 0 {
				break
			}
			if err := txn.Delete(key); err != nil {
				return totalDeleted, err
			}
			elemDeletedInBatch++
		}
		iter.Close()

		// Commit the transaction
		if err := txn.Commit(); err != nil {
			return totalDeleted, err
		}

		// Update the total deleted count
		totalDeleted += elemDeletedInBatch
		_, t, err := state.ParseDataKey(lastKey)
		fmt.Println(lastKey)
		if err != nil {
			return totalDeleted, err
		}
		updateLastDeletedElemDate(t, totalDeleted)

		// If no more items were deleted in this batch, exit the loop
		if elemDeletedInBatch < BATCH_SIZE {
			break
		}
		startKey = lastKey
	}

	txn = state.SetRef.db.NewTransaction(true)
	if err := txn.Delete(state.GetLastDataTimeKey(label)); err != nil {
		return totalDeleted, err
	}
	return totalDeleted, txn.Commit()
}

func (state *AssetState) NewTX(update bool) *badger.Txn {
	return state.SetRef.db.NewTransaction(update)
}

func (state *AssetState) GetInDataRange(t0, t1 pcommon.TimeUnit, txn *badger.Txn, iter *badger.Iterator) (interface{}, error) {
	if t1 < t0 {
		return nil, errors.New("t1 must be after t0")
	}

	label, err := pcommon.Format.TimeFrameToLabel(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return nil, err
	}

	// Open a read-only BadgerDB transaction
	if txn == nil {
		txn = state.SetRef.db.NewTransaction(false)
		defer txn.Discard()
	}

	startKey := state.GetDataKey(label, t0)
	limitKey := state.GetDataKey(label, t1)

	if iter == nil {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		iter = txn.NewIterator(opts)
		defer iter.Close()
	}

	retUnits := UnitTimeArray{}
	retQuantities := QuantityTimeArray{}
	retPoints := PointTimeArray{}
	// Iterate over the keys and retrieve values within the range
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		key := iter.Item().Key()
		if bytes.Compare(key, limitKey) >= 0 {
			break
		}

		_, dataTime, err := state.ParseDataKey(key)
		if err != nil {
			return nil, err
		}

		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		if state.IsUnit() {
			unit := parseRawUnit(value)
			retUnits = append(retUnits, unit.ToTime(dataTime))
		} else if state.IsQuantity() {
			quantity := parseRawQuantity(value)
			retQuantities = append(retQuantities, quantity.ToTime(dataTime))
		} else if state.IsPoint() {
			point, err := parsePoint(value)
			if err != nil {
				return nil, err
			}
			retPoints = append(retPoints, point.ToTime(dataTime))
		} else {
			return nil, errors.New("unknown data type")
		}
	}

	if state.IsUnit() {
		return retUnits, nil
	} else if state.IsQuantity() {
		return retQuantities, nil
	} else if state.IsPoint() {
		return retPoints, nil
	}

	return nil, errors.New("unknown data type")
}

type DataLimitSettings struct {
	TimeFrame      time.Duration
	Limit          int
	OffsetUnixTime pcommon.TimeUnit
	StartByEnd     bool
}

func (state *AssetState) GetDataLimit(settings DataLimitSettings, setARead bool) ([]interface{}, error) {
	timeFrame := settings.TimeFrame
	limit := settings.Limit
	offsetUnixTime := settings.OffsetUnixTime
	startByEnd := settings.StartByEnd

	ret := []interface{}{}

	if limit > 1 && !state.IsTimeframeSupported(timeFrame) {
		return ret, nil
	}

	label, err := pcommon.Format.TimeFrameToLabel(timeFrame)
	if err != nil {
		return nil, err
	}

	// Open a read-only BadgerDB transaction
	txn := state.SetRef.db.NewTransaction(false)
	defer txn.Discard()

	var limitTime pcommon.TimeUnit
	if startByEnd {
		limitTime = state.DataT0()
	} else {
		lastData, rowTime, err := state.GetLatestData(timeFrame)
		if err != nil || lastData == nil {
			return nil, err
		}
		limitTime = rowTime
	}

	startKey := state.GetDataKey(label, offsetUnixTime)
	limitKey := state.GetDataKey(label, limitTime)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Reverse = startByEnd
	if limit > 0 && limit < 100 {
		opts.PrefetchSize = limit
	}

	iter := txn.NewIterator(opts)
	defer iter.Close()

	count := 0

	// Iterate over the keys and retrieve values within the range
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		key := iter.Item().Key()
		if bytes.Equal(key, startKey) {
			continue
		}
		if (startByEnd && bytes.Compare(key, limitKey) < 0) || (!startByEnd && bytes.Compare(key, limitKey) > 0) {
			break
		}

		_, rowTime, err := state.ParseDataKey(key)
		if err != nil {
			return nil, err
		}
		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		var o interface{} = nil

		if state.IsPoint() {
			o, err = parsePoint(value)
			if err != nil {
				return nil, err
			}
			o = o.(Point).ToTime(rowTime)
		} else if state.IsQuantity() {
			o = parseRawQuantity(value)
			o = o.(Quantity).ToTime(rowTime)
		} else if state.IsUnit() {
			o = parseRawUnit(value)
			o = o.(Unit).ToTime(rowTime)
		}
		if o == nil {
			return nil, errors.New("unknown data type")
		}

		if !startByEnd {
			ret = append(ret, o)
		} else {
			ret = append([]interface{}{o}, ret...)
		}

		count += 1
		if count == limit {
			break
		}
	}

	if setARead {
		go func() {
			err := state.updateInReadList(timeFrame)
			if err != nil {
				log.WithFields(log.Fields{
					"symbol": state.SetRef.ID(),
					"error":  err.Error(),
				}).Error("Error setting last read")
			}
		}()
	}

	return ret, nil
}

func (state *AssetState) GetLastStoreBatchTime(timeframe time.Duration) (pcommon.TimeUnit, error) {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return 0, err
	}

	txn := state.SetRef.db.NewTransaction(false)
	defer txn.Discard()

	key := state.GetLastDataTimeKey(label)
	item, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	data, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}

	return pcommon.NewTimeUnitFromIntString(string(data)), nil
}

func (state *AssetState) getSingleData(settings DataLimitSettings) (interface{}, pcommon.TimeUnit, error) {
	settings.Limit = 1
	list, err := state.GetDataLimit(settings, false)
	if err != nil {
		return nil, 0, err
	}
	if len(list) == 0 {
		return nil, 0, nil
	}

	if state.IsPoint() {
		first := list[0].(PointTime)
		return &first.Point, first.Time, nil
	} else if state.IsQuantity() {
		first := list[0].(QuantityTime)
		return &first.Quantity, first.Time, nil
	} else if state.IsUnit() {
		first := list[0].(UnitTime)
		return &first.Unit, first.Time, nil
	}

	return nil, 0, errors.New("unknown data type")

}

func (state *AssetState) GetEarliestData(timeframe time.Duration) (interface{}, pcommon.TimeUnit, error) {
	settings := DataLimitSettings{
		TimeFrame:      timeframe,
		Limit:          1,
		OffsetUnixTime: 0,
		StartByEnd:     false,
	}
	return state.getSingleData(settings)
}

func (state *AssetState) GetLatestData(timeframe time.Duration) (interface{}, pcommon.TimeUnit, error) {
	settings := DataLimitSettings{
		TimeFrame:      timeframe,
		Limit:          1,
		OffsetUnixTime: pcommon.NewTimeUnitFromTime(time.Now()),
		StartByEnd:     true,
	}
	return state.getSingleData(settings)
}

func (state *AssetState) PrintReadList() {
	fmt.Printf("Readlist: %s of %s\n", state.ID(), state.SetRef.ID())
	for _, v := range *state.readList.readList {
		fmt.Println(v.Timeframe, v.Time.ToTime())
	}
	fmt.Println()
	fmt.Println()
}
