package set2

import (
	"bytes"
	"errors"
	"pendulev2/dtype"
	"pendulev2/util"
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

	retUnit := dtype.UnitTimeArray{}
	retQuantity := dtype.QuantityTimeArray{}
	retPoint := dtype.PointTimeArray{}

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
			unit := dtype.ParseRawUnit(value)
			retUnit = append(retUnit, unit.ToTime(dataTime))
		} else if state.IsQuantity() {
			quantity := dtype.ParseRawQuantity(value)
			retQuantity = append(retQuantity, quantity.ToTime(dataTime))
		} else if state.IsPoint() {
			point, err := dtype.ParsePoint(value)
			if err != nil {
				return nil, err
			}
			retPoint = append(retPoint, point.ToTime(dataTime))
		} else {
			return nil, errors.New("unknown data type")
		}
	}

	if state.IsUnit() {
		return retUnit, nil
	} else if state.IsQuantity() {
		return retQuantity, nil
	} else if state.IsPoint() {
		return retPoint, nil
	}
	return nil, errors.New("unknown data type")
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

	retUnits := dtype.UnitTimeArray{}
	retQuantities := dtype.QuantityTimeArray{}
	retPoints := dtype.PointTimeArray{}

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

		if state.IsPoint() {
			p, err := dtype.ParsePoint(value)
			if err != nil {
				return nil, err
			}
			if !startByEnd {
				retPoints = append(retPoints, p.ToTime(rowTime))
			} else {
				retPoints = append([]dtype.PointTime{p.ToTime(rowTime)}, retPoints...)
			}
		} else if state.IsQuantity() {
			q := dtype.ParseRawQuantity(value).ToTime(rowTime)
			if !startByEnd {
				retQuantities = append(retQuantities, q)
			} else {
				retQuantities = append(dtype.QuantityTimeArray{q}, retQuantities...)
			}
		} else if state.IsUnit() {
			u := dtype.ParseRawUnit(value).ToTime(rowTime)
			if !startByEnd {
				retUnits = append(retUnits, u)
			} else {
				retUnits = append(dtype.UnitTimeArray{u}, retUnits...)
			}
		} else {
			return nil, errors.New("unknown data type")
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

	if state.IsUnit() {
		return dtype.DataList(retUnits), nil
	} else if state.IsQuantity() {
		return dtype.DataList(retQuantities), nil
	} else if state.IsPoint() {
		return dtype.DataList(retPoints), nil
	}
	return nil, errors.New("unknown data type")
}

func (state *AssetState) getSingleData(settings DataLimitSettings) (dtype.Data, pcommon.TimeUnit, error) {
	settings.Limit = 1
	list, err := state.GetDataLimit(settings, false)
	if err != nil {
		return nil, 0, err
	}
	len, err := util.Len(list)
	if err != nil {
		return nil, 0, err
	}
	if len == 0 {
		return nil, 0, nil
	}

	if state.IsPoint() {
		cast := list.(dtype.PointTimeArray)
		p := cast[0]
		return &p, p.Time, nil
	} else if state.IsQuantity() {
		cast := list.(dtype.QuantityTimeArray)
		q := cast[0]
		return &q, q.Time, nil
	} else if state.IsUnit() {
		cast := list.(dtype.UnitTimeArray)
		u := cast[0]
		return &u, u.Time, nil
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
