package engine

import (
	setlib "pendulev2/set2"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
)

const (
	STATE_ROLLBACK_KEY = "state-rollback"
)

func buildStateRollbackRunner(state *setlib.AssetState, date string, timeframe time.Duration) *gorunner.Runner {
	runner := gorunner.NewRunner(STATE_ROLLBACK_KEY + "-" + string(state.Address()) + "-" + date)

	addDate(runner, date)
	addTimeframe(runner, timeframe)
	addAssetAddresses(runner, []pcommon.AssetAddress{state.Address()})

	runner.AddProcess(func() error {
		return state.RollbackData(date, timeframe)
	})

	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
		for _, r := range details.RunningRunners {
			if !haveSameAddresses(r, runner) {
				continue
			}
			if !haveSameTimeframe(r, runner) {
				continue
			}
			return false
		}

		return true
	})

	return runner
}
