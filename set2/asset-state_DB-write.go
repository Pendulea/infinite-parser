package set2

import (
	"bytes"
	"errors"
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

func (state *AssetState) Delete(timeFrame time.Duration, t0 pcommon.TimeUnit, updateLastDeletedElemDate func(pcommon.TimeUnit, int)) (int, error) {
	label, err := pcommon.Format.TimeFrameToLabel(timeFrame)
	if err != nil {
		return 0, err
	}

	// Open a BadgerDB transaction
	txn := state.SetRef.db.NewTransaction(true)
	defer txn.Discard()

	isRollback := t0 > state.DataHistoryTime0()

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
		var lastKeySeen []byte
		var lastDeletedElementTime pcommon.TimeUnit = 0
		for iter.Seek(startKey); iter.Valid(); iter.Next() {
			lastKeySeen = iter.Item().KeyCopy(nil)
			if elemDeletedInBatch >= BATCH_SIZE {
				break
			}
			if bytes.Compare(lastKeySeen, limitKey) > 0 {
				break
			}
			_, t, err := state.ParseDataKey(lastKeySeen)
			if err == nil {
				if err := txn.Delete(lastKeySeen); err != nil {
					return totalDeleted, err
				}
				lastDeletedElementTime = t
				elemDeletedInBatch++
			}
		}
		iter.Close()

		// Commit the transaction
		if err := txn.Commit(); err != nil {
			return totalDeleted, err
		}

		// Update the total deleted count
		totalDeleted += elemDeletedInBatch

		if updateLastDeletedElemDate != nil {
			updateLastDeletedElemDate(lastDeletedElementTime, totalDeleted)
		}

		// If no more items were deleted in this batch, exit the loop
		if elemDeletedInBatch < BATCH_SIZE {
			break
		}

		startKey = lastKeySeen
	}

	txn = state.SetRef.db.NewTransaction(true)

	if !isRollback {
		if err := txn.Delete(state.GetLastDataTimeKey(label)); err != nil {
			return totalDeleted, err
		}
		state.setNewConsistencyTime(timeFrame, state.DataHistoryTime0())
	} else {
		state.setNewConsistencyTime(timeFrame, t0-1)
	}
	return totalDeleted, txn.Commit()
}
