package set2

import (
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pcommon "github.com/pendulea/pendule-common"
)

func (state *AssetState) IsConsistent(timeframe time.Duration) (bool, error) {
	t, err := state.GetLastConsistencyTimeCached(timeframe)
	if err != nil {
		return false, err
	}
	minEndAllowed, _ := pcommon.Format.StrDateToDate(pcommon.Format.BuildDateStr(state.consistencyMaxLookbackDays - 1))
	return pcommon.NewTimeUnitFromTime(minEndAllowed) < t, nil
}

func (state *AssetState) ShouldSync() (*string, error) {
	t, err := state.GetLastConsistencyTimeCached(pcommon.Env.MIN_TIME_FRAME)
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

func (state *AssetState) setNewConsistencyTime(timeframe time.Duration, newLastDataTime pcommon.TimeUnit) error {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	tx := state.SetRef.db.NewTransaction(true)
	if err := tx.Set(state.GetLastDataTimeKey(label), []byte(newLastDataTime.String())); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	state.readList.cacheConsistencyUpdate(timeframe, newLastDataTime)
	return nil
}

func (state *AssetState) GetLastConsistencyTimeCached(timeframe time.Duration) (pcommon.TimeUnit, error) {
	c := state.readList.GetConsistency(timeframe)
	if c == nil {
		return 0, errors.New("consistency not found")
	}
	if c[1] == c[0] {
		return 0, nil
	}
	return c[1], nil
}

func (state *AssetState) pullLastConsistencyTimeFromDB(timeframe time.Duration) (pcommon.TimeUnit, error) {
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

func (state *AssetState) __eraseConsistency(timeframe time.Duration) error {
	label, err := pcommon.Format.TimeFrameToLabel(timeframe)
	if err != nil {
		return err
	}

	txn := state.SetRef.db.NewTransaction(true)
	defer txn.Discard()

	if err := txn.Delete(state.GetLastDataTimeKey(label)); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}
