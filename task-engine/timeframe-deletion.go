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
	TIMEFRAME_DELETION_KEY = "timeframe_deletion"
)

func buildTimeFrameDeletionKey(setID string, stateID string, timeframe time.Duration) string {
	label, _ := pcommon.Format.TimeFrameToLabel(timeframe)
	return TIMEFRAME_DELETION_KEY + "-" + setID + "-" + stateID + "-" + label
}

func printTimeframeDeletionStatus(runner *gorunner.Runner, state *setlib.AssetState) {

	label, _ := pcommon.Format.TimeFrameToLabel(getTimeframe(runner))
	TOTAL_DELETED_TICKS := runner.StatValue(STAT_VALUE_DATA_COUNT)

	if runner.IsRunning() {
		if runner.CountSteps() == 0 {
			date := pcommon.NewTimeUnit(runner.Size().Current()).ToTime()

			log.WithFields(log.Fields{
				"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
				"speed":    pcommon.Format.AccurateHumanize(pcommon.TIME_UNIT_DURATION*time.Duration(runner.SizePerMillisecond()*1000)) + " deleted/s",
				"deleted":  pcommon.Format.LargeNumberToShortString(TOTAL_DELETED_TICKS),
				"date":     pcommon.Format.FormatDateStr(date),
				"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
			}).Info(fmt.Sprintf("Deleted %s rows on timeframe: %s (set: %s)", state.ID(), label, state.SetRef.ID()))

		} else if runner.CountSteps() == 1 {
			log.WithFields(log.Fields{
				"deleted": pcommon.Format.LargeNumberToShortString(TOTAL_DELETED_TICKS),
				"done":    "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
			}).Info(fmt.Sprintf("Successfully deleted %s rows on timeframe: %s (set: %s)", state.ID(), label, state.SetRef.ID()))
		}
	}
}

func addTimeframeDeletionRunnerProcess(runner *gorunner.Runner, state *setlib.AssetState, timeframe time.Duration) {

	runner.AddProcess(func() error {
		if !state.IsTimeframeSupported(timeframe) && timeframe != pcommon.Env.MIN_TIME_FRAME {
			return nil
		}

		_, t0, err := state.GetEarliestData(timeframe)
		if err != nil {
			return err
		}
		t1, err := state.GetLastConsistencyTime(timeframe)
		if err != nil {
			return err
		}
		if t0 == t1 || t1 == 0 {
			return nil
		}

		go func() {
			time.Sleep(2 * time.Second)
			for runner.Task.IsRunning() {
				printTimeframeDeletionStatus(runner, state)
				time.Sleep(5 * time.Second)
			}
		}()
		count, err := state.Delete(timeframe, func(lastElementDeletedTime pcommon.TimeUnit, deleted int) {
			runner.SetSize().Current(lastElementDeletedTime.Int(), false)
			runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(deleted))
		})

		if err != nil {
			return err
		}

		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(count))

		if err := state.RemoveInReadList(timeframe); err != nil {
			return err
		}

		runner.AddStep()
		printTimeframeDeletionStatus(runner, state)
		return nil
	})
}

func buildTimeframeDeletionRunner(state *setlib.AssetState, timeframe time.Duration) *gorunner.Runner {
	runner := gorunner.NewRunner(buildTimeFrameDeletionKey(state.SetRef.ID(), state.ID(), timeframe))

	addTimeframe(runner, timeframe)
	addAssetAndSetIDs(runner, []string{state.SetAndAssetID()})

	addTimeframeDeletionRunnerProcess(runner, state, timeframe)

	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
		for _, r := range details.RunningRunners {

			if !haveSameFullIDs(r, runner) {
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
