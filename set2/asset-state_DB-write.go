package set2

import (
	"bytes"
	"errors"
	"math"
	"pendulev2/util"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

func (state *AssetState) Store(data map[pcommon.TimeUnit][]byte, timeframe time.Duration, newPrevState *PrevState, newConsistencyTime pcommon.TimeUnit) error {
	if newConsistencyTime <= 0 {
		return errors.New("newConsistencyTime must be greater than 0")
	}

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

	if err := state.storePrevState(newPrevState, timeframe, newConsistencyTime); err != nil {
		return err
	}

	if err := state.setNewConsistencyTime(timeframe, newConsistencyTime); err != nil {
		return err
	}

	return nil
}

func (state *AssetState) rollback(
	timeFrame time.Duration,
	toDateAsT0 string,
	updateLastDeletedElemDate func(pcommon.TimeUnit, int)) (int, error) {

	label, err := pcommon.Format.TimeFrameToLabel(timeFrame)
	if err != nil {
		return 0, err
	}

	dateT, err := pcommon.Format.StrDateToDate(toDateAsT0)
	if err != nil {
		return 0, err
	}

	t0 := pcommon.NewTimeUnitFromTime(dateT)

	//Either we rollback the data to a certain date, or we remove all the data
	isRollback := strings.Compare(toDateAsT0, state.Settings().MinDataDate) > 0

	/* Formating t0 */
	// If the date is a rollback and timeFrame is greater than 1 day, adjust t0 correctly
	if timeFrame > time.Hour*24 && isRollback {
		consistencyTime, err := state.GetLastConsistencyTimeCached(timeFrame)
		if err != nil {
			return 0, err
		}
		diff := consistencyTime - t0
		cycle := math.Floor(float64(diff) / float64(pcommon.NewTimeUnit(0).Add(timeFrame)))

		//adjusting t0 to the last consistency time if timeFrame is greater than 1 day
		t0 = consistencyTime.Add(-(timeFrame * time.Duration(cycle)))

		// If the date is not a rollback, set the t0 to the beginning of the data history
	} else if !isRollback {
		t0 = state.DataHistoryTime0()
	}

	// Create an iterator to iterate over the keys
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true

	// Total number of elements deleted
	totalDeleted := 0

	//we iterate from most recent ticks to the oldest
	startKey := state.GetDataKey(label, pcommon.NewTimeUnitFromTime(time.Now()))
	limitKey := state.GetDataKey(label, t0)

	//we keep track of the dates we have seen to delete prev states
	dateSeen := map[string]bool{}

	txn := state.SetRef.db.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(opts)
	defer iter.Close()

	//element destructor
	destructor := util.NewDestructor(state.SetRef.db)

	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		currentKey := iter.Item().KeyCopy(nil)
		if bytes.Compare(currentKey, limitKey) < 0 {
			break
		}

		if _, elemTime, err := state.ParseDataKey(currentKey); err == nil {
			destructor.Delete(currentKey)
			totalDeleted++

			if updateLastDeletedElemDate != nil {
				updateLastDeletedElemDate(elemTime, totalDeleted)
			}

			date := pcommon.Format.FormatDateStr(elemTime.ToTime())
			if _, ok := dateSeen[date]; !ok {
				if len(dateSeen) == 0 {
					dateTime, _ := pcommon.Format.StrDateToDate(date)
					nextDate := pcommon.Format.FormatDateStr(dateTime.Add(time.Hour * 24))
					destructor.Delete(state.GetPrevStateKey(label, nextDate))
				}
				dateSeen[date] = true
				destructor.Delete(state.GetPrevStateKey(label, date))

				if err := state.setNewConsistencyTime(timeFrame, elemTime); err != nil {
					return totalDeleted, err
				}
			}
		}
		if destructor.Error() != nil {
			destructor.Discard()
			return totalDeleted, destructor.Error()
		}
	}

	destructor.Discard()
	if destructor.Error() != nil {
		return totalDeleted, destructor.Error()
	}

	//If this is not a rollback, delete the last data time key
	if !isRollback {
		if err := state.__eraseConsistency(timeFrame); err != nil {
			return totalDeleted, err
		}

		state.readList.cachePrevStateUpdate(timeFrame, NewAssetPrevState())

		if timeFrame != pcommon.Env.MIN_TIME_FRAME {
			if err := state.RemoveInReadList(timeFrame); err != nil {
				return totalDeleted, err
			}
		} else {
			state.readList.cacheConsistencyUpdate(timeFrame, state.DataHistoryTime0())
		}

	} else {

		if err := state.setNewConsistencyTime(timeFrame, t0); err != nil {
			return totalDeleted, err
		}

		//update prevState in cache
		lastState, err := state.pullLastPrevStateFromDB(timeFrame)
		if err != nil {
			return totalDeleted, err
		}

		state.readList.cachePrevStateUpdate(timeFrame, lastState)
	}

	return totalDeleted, nil
}
