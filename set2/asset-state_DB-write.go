package set2

import (
	"bytes"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

func (state *AssetState) Store(data map[pcommon.TimeUnit][]byte, timeframe time.Duration, newConsistencyTime pcommon.TimeUnit) error {
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

	if newConsistencyTime > 0 {
		if err := state.SetNewConsistencyTime(timeframe, newConsistencyTime, nil); err != nil {
			return err
		}
	}

	return nil
}

func (state *AssetState) Delete(timeFrame time.Duration, updateLastDeletedElemDate func(pcommon.TimeUnit, int)) (int, error) {
	label, err := pcommon.Format.TimeFrameToLabel(timeFrame)
	if err != nil {
		return 0, err
	}

	t0 := state.DataHistoryTime0()

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

func (state *AssetState) StorePrevState(data []byte, timeframe time.Duration) error {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	txn := state.NewTX(true)
	defer txn.Discard()

	if err := txn.Set(state.GetPrevStateKey(label), data); err != nil {
		return err
	}
	return txn.Commit()
}
