package engine

import (
	"fmt"
	setlib "pendulev2/set2"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	STATE_ROLLBACK_KEY = "state-rollback"
)

func buildStateRollbackRunner(state *setlib.AssetState, date string, timeframe time.Duration) *gorunner.Runner {
	label, _ := pcommon.Format.TimeFrameToLabel(timeframe)
	runner := gorunner.NewRunner(STATE_ROLLBACK_KEY + "-" + string(state.Address()) + "-" + date + "-" + label)

	addDate(runner, date)
	addTimeframe(runner, timeframe)
	addAssetAddresses(runner, []pcommon.AssetAddress{state.Address()})

	runner.AddProcess(func() error {
		err := state.RollbackData(date, timeframe, func(percent float64) {
			log.WithFields(log.Fields{
				"timeframe": label,
				"progress":  fmt.Sprintf("%.2f", percent) + "%",
			}).Info(fmt.Sprintf("Rollingback %s to %s", state.ParsedAddress().PrettyString(), date+" 00:00:00"))
		})
		if err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"timeframe": label,
			"done":      "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
		}).Info(fmt.Sprintf("Rolledback %s to %s", state.ParsedAddress().PrettyString(), date+" 00:00:00"))
		return nil
	})

	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
		for _, r := range details.RunningRunners {
			if !haveSameAddresses(r, runner) {
				continue
			}
			if !haveSameTimeframe(r, runner) {
				continue
			}
			list := getDepAddresses(getAddresses(runner)[0])
			i := 0
			for _, addr := range list {
				if isAddressInRunner(r, addr) {
					i++
				}
			}
			if i == 0 {
				continue
			}
			return false
		}

		return true
	})

	return runner
}
