package engine

import (
	"errors"
	"fmt"
	setlib "pendulev2/set2"
	"pendulev2/util"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	TIMEFRAME_INDEXING_KEY = "timeframe_indexing"
)

func buildTimeFrameIndexingKey(setID string, stateID string, timeframe time.Duration) string {
	label, _ := pcommon.Format.TimeFrameToLabel(timeframe)
	return TIMEFRAME_INDEXING_KEY + "-" + setID + "-" + stateID + "-" + label
}

func printTimeframeIndexingStatus(runner *gorunner.Runner, state *setlib.AssetState) {
	label, _ := pcommon.Format.TimeFrameToLabel(getTimeframe(runner))

	PARSED_ROWS_COUNT := runner.StatValue(STAT_VALUE_DATA_COUNT)
	if runner.IsRunning() {
		if runner.CountSteps() == 0 {
			date := pcommon.NewTimeUnit(runner.Size().Current()).ToTime()

			log.WithFields(log.Fields{
				"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
				"speed":    pcommon.Format.AccurateHumanize(pcommon.TIME_UNIT_DURATION*time.Duration(runner.SizePerMillisecond()*1000)) + " indexed/s",
				"rows":     pcommon.Format.LargeNumberToShortString(PARSED_ROWS_COUNT),
				"date":     pcommon.Format.FormatDateStr(date),
				"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
			}).Info(fmt.Sprintf("Indexing new %s rows on timeframe: %s (set: %s)", state.ID(), label, state.SetRef.ID()))

		} else if runner.CountSteps() == 1 {
			log.WithFields(log.Fields{
				"rows": pcommon.Format.LargeNumberToShortString(PARSED_ROWS_COUNT),
				"done": "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
			}).Info(fmt.Sprintf("Successfully stored %s rows on timeframe: %s (set: %s)", state.ID(), label, state.SetRef.ID()))
		}
	}
}

func addTimeframeIndexingRunnerProcess(runner *gorunner.Runner, state *setlib.AssetState) {

	process := func() error {

		if state.IsPoint() {
			return nil
		}

		task := runner.Task
		timeframe, _ := gorunner.GetArg[time.Duration](task.Args, ARG_VALUE_TIMEFRAME)

		sync, err := state.IsTimeframeIndexUpToDate(timeframe)
		if err != nil {
			return err
		}
		if sync {
			return nil
		}

		maxTime, err := state.IsConsistentUntil(pcommon.Env.MIN_TIME_FRAME)
		if err != nil {
			return err
		}

		if err := state.AddIfUnfoundInReadList(timeframe); err != nil {
			return err
		}

		go func() {
			for runner.Task.IsRunning() {
				time.Sleep(5 * time.Second)
				printTimeframeIndexingStatus(runner, state)
			}
		}()

		prevT1, err := state.GetLastTimeframeIndexingDate(timeframe)
		if err != nil {
			return err
		}
		var t0, t1 pcommon.TimeUnit
		if prevT1 == 0 {
			t0Time, t1Time := buildInitialCandleRange(state.DataT0().ToTime(), timeframe)
			t0 = pcommon.NewTimeUnitFromTime(t0Time)
			t1 = pcommon.NewTimeUnitFromTime(t1Time)
		} else {
			t0 = prevT1
			t1 = t0.Add(timeframe)
		}

		const MAX_BATCH_SIZE = 30_000
		const MAX_READ_SIZE = 1_000_000

		runner.SetSize().Initial(t0.Int())
		runner.SetSize().Max(maxTime.Int())

		batch := make(map[pcommon.TimeUnit][]byte)
		currentReadSize := 0

		//tx
		txn := state.NewTX(false)
		defer txn.Discard()

		//iterator
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		iter := txn.NewIterator(opts)
		defer iter.Close()

		for t1 < maxTime {
			ticks, err := state.GetInDataRange(t0, t1, txn, iter)
			if err != nil {
				return err
			}
			size, _ := util.Len(ticks)
			currentReadSize += size

			if size > 0 {
				if state.IsUnit() {
					list := ticks.(setlib.UnitTimeArray)
					ret := setlib.AggregateUnits(list, false)
					batch[t1] = ret.ToRaw(state.Precision())
				} else if state.IsQuantity() {
					list := ticks.(setlib.QuantityTimeArray)
					ret := setlib.AggregateQuantities(list)
					batch[t1] = ret.ToRaw(state.Precision())
				} else {
					return errors.New("unsupported state type")
				}
				runner.IncrementStatValue(STAT_VALUE_DATA_COUNT, 1)
			}
			if len(batch) >= MAX_BATCH_SIZE || currentReadSize >= MAX_READ_SIZE {
				if err := state.Store(batch, timeframe, t1); err != nil {
					return err
				}
				batch = make(map[pcommon.TimeUnit][]byte)
				currentReadSize = 0
			}

			t0 = t1
			t1 = t1.Add(timeframe)
			runner.SetSize().Current(t0.Int(), false)

			if runner.MustInterrupt() {
				break
			}
		}

		if len(batch) > 0 {
			if err := state.Store(batch, timeframe, t0); err != nil {
				return err
			}
		}

		runner.AddStep()
		printTimeframeIndexingStatus(runner, state)

		return nil
	}

	runner.AddProcess(process)
}

func buildTimeframeIndexingRunner(state *setlib.AssetState, timeframe time.Duration) *gorunner.Runner {
	runner := gorunner.NewRunner(buildTimeFrameIndexingKey(state.SetRef.ID(), state.ID(), timeframe))

	runner.Task.AddArgs(ARG_VALUE_TIMEFRAME, timeframe)
	runner.AddArgs(ARG_VALUE_SET_ID, state.SetRef.ID())
	runner.AddArgs(ARG_VALUE_ASSETS, state.ID)

	addTimeframeIndexingRunnerProcess(runner, state)

	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
		for _, r := range details.RunningRunners {

			if !haveSameSetID(r, runner) {
				continue
			}

			if !haveSameTimeframe(r, runner) {
				continue
			}

			if !haveCommonAssets(r, runner) {
				continue
			}

			return false
		}
		return true
	})

	return runner
}

func buildInitialCandleRange(earliestCandle time.Time, timeframe time.Duration) (time.Time, time.Time) {
	earliestCandle = earliestCandle.UTC()

	nextQuarter := func(t time.Time) time.Time {
		quarter := int(t.Month()-1) / 3
		year := t.Year()
		month := quarter*3 + 1
		if month > 12 {
			month = 1
			year++
		}
		return time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	}

	nextNMonth := func(t time.Time, n int) time.Time {
		year := t.Year()
		month := int(t.Month()) + n
		if month > 12 {
			month = month % 12
			year++
		}
		return time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	}

	nextNWeek := func(t time.Time, n int) time.Time {
		offset := int(time.Monday - t.Weekday())
		if offset <= 0 {
			offset += 7 // Ensure it's the next week
		}
		nextWeekStart := t.AddDate(0, 0, offset)
		if n > 1 {
			nextWeekStart = nextWeekStart.AddDate(0, 0, 7*(n-1))
		}
		return time.Date(nextWeekStart.Year(), nextWeekStart.Month(), nextWeekStart.Day(), 0, 0, 0, 0, time.UTC)
	}

	nextNDay := func(t time.Time, n int) time.Time {
		d1 := t.AddDate(0, 0, n)
		//midnight of the next day
		d1 = time.Date(d1.Year(), d1.Month(), d1.Day(), 0, 0, 0, 0, time.UTC)
		return d1
	}

	nextHour := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
	}

	nextMinute := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, time.UTC)
	}

	if timeframe == pcommon.QUARTER {
		return earliestCandle, nextQuarter(earliestCandle)
	} else if timeframe%pcommon.MONTH == 0 {
		return earliestCandle, nextNMonth(earliestCandle, int(timeframe/pcommon.MONTH))
	} else if timeframe%pcommon.WEEK == 0 {
		return earliestCandle, nextNWeek(earliestCandle, int(timeframe/pcommon.WEEK))
	} else if timeframe%pcommon.DAY == 0 {
		return earliestCandle, nextNDay(earliestCandle, int(timeframe/pcommon.DAY))
	} else if timeframe%time.Hour == 0 {
		if timeframe == time.Hour {
			return earliestCandle, nextHour(earliestCandle)
		}
		//if midnight
		if earliestCandle.Hour() == 0 && earliestCandle.Minute() == 0 && earliestCandle.Second() == 0 {
			return earliestCandle, earliestCandle.Add(timeframe)
		}

		t0 := nextNDay(earliestCandle, 1)
		return t0, t0.Add(timeframe)

	} else if timeframe%time.Minute == 0 {
		if timeframe == time.Minute {
			return earliestCandle, nextMinute(earliestCandle)
		}

		if earliestCandle.Hour() == 0 && earliestCandle.Minute() == 0 && earliestCandle.Second() == 0 {
			return earliestCandle, earliestCandle.Add(timeframe)
		}

		t0 := nextNDay(earliestCandle, 1)
		return t0, t0.Add(timeframe)
	} else {
		return earliestCandle, earliestCandle.Add(timeframe)
	}
}
