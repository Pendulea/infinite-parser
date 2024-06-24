package set2

import (
	"bytes"
	"errors"
	"pendulev2/dtype"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

func (state *AssetState) NewTX(update bool) *badger.Txn {
	return state.SetRef.db.NewTransaction(update)
}

func (state *AssetState) GetInDataRange(t0, t1 pcommon.TimeUnit, timeframe time.Duration, txn *badger.Txn, iter *badger.Iterator) (dtype.DataList, error) {
	if t1 < t0 {
		return nil, errors.New("t1 must be after t0")
	}

	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
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
		opts.Reverse = false
		opts.PrefetchValues = true
		iter = txn.NewIterator(opts)
		defer iter.Close()
	}

	ret := dtype.NewTypeTimeArray(state.Type())

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

		unraw, err := dtype.ParseTypeData(state.Type(), value, dataTime)
		if err != nil {
			return nil, err
		}
		ret = ret.Append(unraw)
	}

	return ret, nil
}

type DataLimitSettings struct {
	TimeFrame      time.Duration
	Limit          int
	OffsetUnixTime pcommon.TimeUnit
	StartByEnd     bool
}

func (state *AssetState) GetDataLimit(settings DataLimitSettings, setARead bool) (dtype.DataList, error) {
	timeFrame := settings.TimeFrame
	limit := settings.Limit
	offsetUnixTime := settings.OffsetUnixTime
	startByEnd := settings.StartByEnd

	ret := dtype.NewTypeTimeArray(state.Type())

	if limit > 1 && !state.IsTimeframeSupported(timeFrame) {
		return nil, nil
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

		unraw, err := dtype.ParseTypeData(state.Type(), value, rowTime)
		if err != nil {
			return nil, err
		}

		if !startByEnd {
			ret = ret.Append(unraw)
		} else {
			ret = ret.Prepend(unraw)
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

func (state *AssetState) getSingleData(settings DataLimitSettings) (dtype.Data, pcommon.TimeUnit, error) {
	settings.Limit = 1
	list, err := state.GetDataLimit(settings, false)
	if err != nil {
		return nil, 0, err
	}
	if list.Len() == 0 {
		return nil, 0, nil
	}

	first := list.First()
	return first, first.GetTime(), nil
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
