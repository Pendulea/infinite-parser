package engine

import (
	"math"
	"pendulev2/set2"
	setlib "pendulev2/set2"
	"reflect"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
)

func addIndicatorIndexingRunnerProcess(runner *gorunner.Runner, state *setlib.AssetState) {
	const MAX_BATCH_SIZE = 30_000

	process := func() error {

		// If the state is a point and has no dependencies, we don't need to index it
		if !state.IsPoint() && !state.ParsedAddress().HasDependencies() {
			return nil
		}

		timeframe := getTimeframe(runner)

		//get the previous state
		prevIndicatorState, err := state.GetPrevState(timeframe)
		if err != nil {
			return err
		}

		//calculate the minimum time of the dependencies
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

		//if the minimum time of the dependencies is greater than the current time, we don't need to index
		if minDependenciesTime > pcommon.NewTimeUnitFromTime(time.Now()) {
			return nil
		}

		if timeframe != pcommon.Env.MIN_TIME_FRAME {
			if err := state.AddIfUnfoundInReadList(timeframe); err != nil {
				return err
			}
		}

		consistentTime, err := state.GetLastConsistencyTime(timeframe)
		if err != nil {
			return err
		}
		//if the last consistency time is equal than the minimum time of the dependencies, we don't need to index
		if minDependenciesTime <= consistentTime {
			return nil
		}

		//log every 5 seconds the indexing status
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
		//if there is no previous indexing
		if prevT1 == 0 {
			//we build the initial candle range
			t0Time, t1Time := buildInitialCandleRange(state.DataHistoryTime0().ToTime(), timeframe)
			t0 = pcommon.NewTimeUnitFromTime(t0Time)
			t1 = pcommon.NewTimeUnitFromTime(t1Time)
			//we instantiate the previous list
			for index, dep := range state.DependenciesRef {
				prevList[index] = pcommon.NewTypeTimeArray(dep.DataType())
			}

			//if there is a previous indexing
		} else {
			//we set the previous indexing date to t0
			t0 = prevT1
			t1 = t0.Add(timeframe)

			//we get the previous ticks for each dependency, in case we need to cumulate them
			for index, dep := range state.DependenciesRef {
				settings := set2.DataLimitSettings{
					TimeFrame:      timeframe,
					Limit:          10,
					StartByEnd:     true,
					OffsetUnixTime: t0.Add(timeframe),
				}
				ticks, err := dep.GetDataLimit(settings, false)
				if err != nil {
					return err
				}
				prevList[index] = ticks
			}
		}

		//we set the the min and max size of the runner
		runner.SetSize().Initial(t0.Int())
		runner.SetSize().Max(minDependenciesTime.Int())

		//we build the indicator data builder
		b := pcommon.NewIndicatorDataBuilder(state.Type(), prevIndicatorState, state.ParsedAddress().Arguments)

		for t1 < minDependenciesTime {
			batch := pcommon.PointTimeArray{}
			currentList := make([]pcommon.DataList, len(state.DependenciesRef))

			//we get the ticks for each dependency
			for index, dep := range state.DependenciesRef {

				//interval duration between t0 and the minimum time of the dependencies (end of the indexing)
				totalIntervalDuration := time.Duration((minDependenciesTime - t0).Int() * int64(pcommon.TIME_UNIT_DURATION))
				//we calculate the number of ticks in the interval
				maxBatchSize := totalIntervalDuration / timeframe
				//we take the minimum between the maxBatchSize and the MAX_BATCH_SIZE
				maxTickCount := int(math.Min(float64(maxBatchSize), float64(MAX_BATCH_SIZE)))

				//we calculate the end of the interval
				t1 = t0.Add(timeframe * time.Duration(maxTickCount))

				//we get the ticks between t0 and newT1
				ticks, err := dep.GetInDataRange(t0, t1, timeframe, nil, nil)
				if err != nil {
					return err
				}
				currentList[index] = ticks
			}

			for {
				//we get the earliest tick of the dependencies
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

				//if there is no earliest tick, we break
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

			if err := state.StorePrevState(b.PrevState(), timeframe); err != nil {
				return err
			}

			if err := state.Store(batch.ToRaw(state.Decimals()), timeframe, t1); err != nil {
				return err
			}

			if runner.MustInterrupt() {
				break
			}

			t0 = t1
			t1 = t1.Add(timeframe)
			runner.SetSize().Current(t0.Int(), false)
		}
		runner.AddStep()
		printTimeframeIndexingStatus(runner, state)
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
