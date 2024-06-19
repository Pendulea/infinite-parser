package set2

import (
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

type AssetStates []*AssetState

func (states AssetStates) LeastConsistent(timeframe time.Duration) (*AssetState, error) {
	leastConsistentTime := pcommon.NewTimeUnitFromTime(time.Now())
	var ret *AssetState = nil

	for _, state := range states {
		t, err := state.GetLastConsistencyTime(timeframe)
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
