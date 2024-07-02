package set2

import (
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

func (state *AssetState) IsConsistent(timeframe time.Duration) (bool, error) {
	t, err := state.GetLastConsistencyTime(timeframe)
	if err != nil {
		return false, err
	}
	minEndAllowed, _ := pcommon.Format.StrDateToDate(pcommon.Format.BuildDateStr(state.consistencyMaxLookbackDays - 1))
	return pcommon.NewTimeUnitFromTime(minEndAllowed) < t, nil
}

func (state *AssetState) ShouldSync() (*string, error) {
	t, err := state.GetLastConsistencyTime(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return nil, err
	}

	if t == 0 {
		return &state.settings.MinDataDate, nil
	}

	offset := time.Duration(state.consistencyMaxLookbackDays-1) * 24 * time.Hour

	max := pcommon.NewTimeUnitFromTime(time.Now()).Add(-offset)
	if t < max {
		s := pcommon.Format.FormatDateStr(t.ToTime())
		return &s, nil
	}
	return nil, nil
}

func (state *AssetState) SetNewConsistencyTime(timeframe time.Duration, newLastDataTime pcommon.TimeUnit, tx *badger.Txn) error {
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

func (state *AssetState) GetLastConsistencyTime(timeframe time.Duration) (pcommon.TimeUnit, error) {
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
