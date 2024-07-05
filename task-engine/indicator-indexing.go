package engine

import (
	"math"
	"pendulev2/set2"
	setlib "pendulev2/set2"
	"reflect"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
)

func addIndicatorIndexingRunnerProcess(runner *gorunner.Runner, state *setlib.AssetState) {
	const MAX_BATCH_SIZE = 30_000

	process := func() error {

		if !state.IsPoint() && !state.ParsedAddress().HasDependencies() {
			return nil
		}

		timeframe := getTimeframe(runner)
		prevIndicatorState, err := state.GetPrevState(timeframe)
		if err != nil {
			return err
		}

		minDependenciesTime := pcommon.TimeUnit(math.MaxInt64)
		for _, dep := range state.DependenciesRef {
			maxTimeDep, err := dep.GetLastConsistencyTime(timeframe)
			if err != nil {
				return err
			}
			if maxTimeDep < minDependenciesTime {
				minDependenciesTime = maxTimeDep
			}
		}

		if timeframe == pcommon.Env.MIN_TIME_FRAME {
			consistentTime, err := state.GetLastConsistencyTime(timeframe)
			if err != nil {
				return err
			}
			if minDependenciesTime <= consistentTime {
				return nil
			}
		} else {
			if err := state.AddIfUnfoundInReadList(timeframe); err != nil {
				return err
			}
			sync, err := state.IsTimeframeIndexUpToDate(timeframe)
			if err != nil {
				return err
			}
			if sync {
				return nil
			}
		}

		go func() {
			for runner.Task.IsRunning() {
				time.Sleep(5 * time.Second)
				printTimeframeIndexingStatus(runner, state)
			}
		}()

		prevList := make([]pcommon.DataList, len(state.DependenciesRef))

		prevT1, err := state.GetLastTimeframeIndexingDate(timeframe)
		if err != nil {
			return err
		}
		var t0, t1 pcommon.TimeUnit
		if prevT1 == 0 {
			t0Time, t1Time := buildInitialCandleRange(state.DataHistoryTime0().ToTime(), timeframe)
			t0 = pcommon.NewTimeUnitFromTime(t0Time)
			t1 = pcommon.NewTimeUnitFromTime(t1Time)
			for index, dep := range state.DependenciesRef {
				prevList[index] = pcommon.NewTypeTimeArray(dep.DataType())
			}

		} else {
			t0 = prevT1
			t1 = t0.Add(timeframe)

			for index, dep := range state.DependenciesRef {
				settings := set2.DataLimitSettings{
					TimeFrame:      timeframe,
					Limit:          MAX_BATCH_SIZE,
					StartByEnd:     true,
					OffsetUnixTime: t1.Add(timeframe),
				}
				ticks, err := dep.GetDataLimit(settings, false)
				if err != nil {
					return err
				}
				prevList[index] = ticks
			}
		}

		runner.SetSize().Initial(t0.Int())
		runner.SetSize().Max(minDependenciesTime.Int())

		b := pcommon.NewIndicatorDataBuilder(state.Type(), prevIndicatorState, state.ParsedAddress().Arguments)

		for t1 < minDependenciesTime {
			batch := pcommon.PointTimeArray{}

			isLastBatch := false
			currentList := make([]pcommon.DataList, len(state.DependenciesRef))
			for index, dep := range state.DependenciesRef {
				settings := set2.DataLimitSettings{
					TimeFrame:      timeframe,
					Limit:          MAX_BATCH_SIZE,
					StartByEnd:     false,
					OffsetUnixTime: t1,
				}
				ticks, err := dep.GetDataLimit(settings, false)
				if err != nil {
					return err
				}
				currentList[index] = ticks
			}

			if lo.EveryBy(currentList, func(ticks pcommon.DataList) bool {
				return !(ticks.Len() < MAX_BATCH_SIZE-1)
			}) {
				isLastBatch = true
			}

			for {
				minTime := pcommon.TimeUnit(math.MaxInt64)
				var minDependency *setlib.AssetState = nil

				for dependencyIndex, ticks := range currentList {
					first := ticks.First()
					if first != nil {
						if first.GetTime() < minTime {
							minTime = first.GetTime()
							minDependency = state.DependenciesRef[dependencyIndex]
						}
					}
				}

				if minDependency == nil {
					break
				}

				dataToCumulate := []pcommon.Data{}
				for dependencyIndex, ticks := range currentList {
					//we get the earliest tick of the dependency
					first := ticks.First()
					//if there is no earliest tick or the earliest tick is after the minTime
					if first == nil || first.GetTime() > minTime {
						//we look for it in the previous list
						last := prevList[dependencyIndex].Last()

						if last == nil {
							break
						}

						//then we add it to the data to cumulate
						dataToCumulate = append(dataToCumulate, unpointerData(last)) //last shouldn't be a pointer currently it is a pointer to interface

						//if the earliest tick is equal to the minTime
					} else {
						//we add it to the data to cumulate
						dataToCumulate = append(dataToCumulate, unpointerData(first))
						//we remove it from the current list
						currentList[dependencyIndex] = ticks.RemoveFirstN(1)
						//we add it to the previous list
						prevList[dependencyIndex] = prevList[dependencyIndex].Prepend(unpointerData(first))
						if prevList[dependencyIndex].Len() > 1 {
							prevList[dependencyIndex] = prevList[dependencyIndex].RemoveFirstN(1)
						}
					}
				}

				//if we have cumulated all the data
				if len(dataToCumulate) == len(state.DependenciesRef) {
					p, err := b.ComputeUnsafe(dataToCumulate...)
					if err != nil {
						return err
					}
					batch = append(batch, p.ToTime(minTime))
				}
			}

			runner.IncrementStatValue(STAT_VALUE_DATA_COUNT, int64(len(batch)))

			if err := state.Store(batch.ToRaw(state.Decimals()), timeframe, t1); err != nil {
				return err
			}
			if isLastBatch {
				break
			}
		}
		runner.AddStep()
		return nil
	}

	runner.AddProcess(process)

}

func buildIndicatorIndexingRunner(asset *setlib.AssetState, timeframe time.Duration) *gorunner.Runner {
	runner := gorunner.NewRunner(buildTimeFrameIndexingKey(asset.Address(), timeframe))

	addTimeframe(runner, timeframe)
	addAssetAddresses(runner, []pcommon.AssetAddress{asset.Address()})

	addIndicatorIndexingRunnerProcess(runner, asset)

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

func unpointerData(d pcommon.Data) pcommon.Data {
	value := reflect.ValueOf(d)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	// Ensure last is not a pointer
	return value.Interface().(pcommon.Data)
}
