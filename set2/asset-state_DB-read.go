package set2

import (
	"bytes"
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

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
