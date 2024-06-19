package set2

import (
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

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

func (state *AssetState) GetActiveTimeFrameList() []time.Duration {
	return state.readList.GetTimeFrameList()
}
