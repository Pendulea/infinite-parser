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

func addIndicatorIndexingRunnerProcess(runner *gorunner.Runner, asset *setlib.AssetState) {
	const MAX_BATCH_SIZE = 30_000

	process := func() error {

		timeframe := getTimeframe(runner)

		// If the state is a point and has no dependencies, we don't need to index it
		if !asset.IsPoint() && !asset.ParsedAddress().HasDependencies() {
			return nil
		}

		//calculate the minimum time of the dependencies
		minLastDependenciesTime := pcommon.TimeUnit(math.MaxInt64)
		for _, dep := range asset.DependenciesRef {
			maxTimeDep, err := dep.GetLastConsistencyTimeCached(timeframe)
			if err != nil {
				return err
			}
			if maxTimeDep < minLastDependenciesTime {
				minLastDependenciesTime = maxTimeDep
			}
		}

		//if the minimum time of the dependencies is greater than the current time, we don't need to index
		if minLastDependenciesTime > pcommon.NewTimeUnitFromTime(time.Now()) {
			return nil
		}

		if err := asset.AddIfUnfoundInReadList(timeframe); err != nil {
			return err
		}

		consistentTime, err := asset.GetLastConsistencyTimeCached(timeframe)
		if err != nil {
			return err
		}

		//if the last consistency time is equal than the minimum time of the dependencies, we don't need to index
		if minLastDependenciesTime == consistentTime {
			return nil
		}

		//get the previous state
		prevState, err := asset.GetLastPrevStateCached(timeframe)
		if err != nil {
			return err
		}

		//log every 5 seconds the indexing status
		go func() {
			for runner.Task.IsRunning() {
				time.Sleep(5 * time.Second)
				printTimeframeIndexingStatus(runner, asset)
			}
		}()

		prevList := make([]pcommon.DataList, len(asset.DependenciesRef))

		prevT1, err := asset.GetLastTimeframeIndexingDate(timeframe)
		if err != nil {
			return err
		}

		var t0, t1 pcommon.TimeUnit
		//if there is no previous indexing
		if prevT1 == 0 {
			//we build the initial candle range
			t0Time, t1Time := buildInitialCandleRange(asset.DataHistoryTime0().ToTime(), timeframe)
			t0 = pcommon.NewTimeUnitFromTime(t0Time)
			t1 = pcommon.NewTimeUnitFromTime(t1Time)
			//we instantiate the previous list
			for index, dep := range asset.DependenciesRef {
				prevList[index] = pcommon.NewTypeTimeArray(dep.DataType())
			}

			//if there is a previous indexing
		} else {
			//we set the previous indexing date to t0
			t0 = prevT1
			t1 = t0.Add(timeframe)

			//we get the previous ticks for each dependency, in case we need to cumulate them
			for index, dep := range asset.DependenciesRef {
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
		runner.SetSize().Max(minLastDependenciesTime.Int())

		//we build the indicator data builder
		indicatorDataBuilder := pcommon.NewIndicatorDataBuilder(asset.Type(), prevState.State(), asset.ParsedAddress().Arguments, asset.Decimals())

		for t1 <= minLastDependenciesTime {
			batch := pcommon.PointTimeArray{}
			currentList := make([]pcommon.DataList, len(asset.DependenciesRef))

			//we get the ticks for each dependency
			for index, dep := range asset.DependenciesRef {

				//interval duration between t0 and the minimum time of the dependencies (end of the indexing)
				totalIntervalDuration := time.Duration((minLastDependenciesTime - t0).Int() * int64(pcommon.TIME_UNIT_DURATION))
				//we calculate the number of ticks in the interval
				maxBatchSize := totalIntervalDuration / timeframe
				//we take the minimum between the maxBatchSize and the MAX_BATCH_SIZE
				maxTickCount := int(math.Min(float64(maxBatchSize), float64(MAX_BATCH_SIZE)))

				//we calculate the end of the interval
				t1 = t0.Add(timeframe * time.Duration(maxTickCount))

				//we get the ticks between t0 and newT1
				ticks, err := dep.GetInDataRange(t0, t1.Add(time.Millisecond), timeframe, nil, nil, false)
				if err != nil {
					return err
				}
				currentList[index] = ticks
			}

			for {
				//we get the earliest tick of the dependencies
				earliestTime := pcommon.TimeUnit(math.MaxInt64)
				var minDependency *setlib.AssetState = nil
				for dependencyIndex, ticks := range currentList {
					first := ticks.First()
					if first != nil {
						if first.GetTime() < earliestTime {
							earliestTime = first.GetTime()
							minDependency = asset.DependenciesRef[dependencyIndex]
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
					if first == nil || first.GetTime() > earliestTime {
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
				if len(dataToCumulate) == len(asset.DependenciesRef) {
					p, err := indicatorDataBuilder.ComputeUnsafe(dataToCumulate...)
					if err != nil {
						return err
					}
					prevState.CheckUpdateMin(p.Value, earliestTime)
					prevState.CheckUpdateMax(p.Value, earliestTime)
					batch = append(batch, p.ToTime(earliestTime))

					currentDate := pcommon.Format.FormatDateStr(earliestTime.ToTime())
					nextDate := pcommon.Format.FormatDateStr(earliestTime.Add(timeframe).ToTime())

					if currentDate != nextDate {
						prevState.UpdateState(indicatorDataBuilder.PrevState())
						if err := asset.Store(batch.ToRaw(asset.Decimals()), timeframe, prevState.Copy(), earliestTime); err != nil {
							return err
						}
						runner.IncrementStatValue(STAT_VALUE_DATA_COUNT, int64(len(batch)))
						batch = pcommon.PointTimeArray{}
					}
				}
			}

			if batch.Len() > 0 {
				prevState.UpdateState(indicatorDataBuilder.PrevState())
				if err := asset.Store(batch.ToRaw(asset.Decimals()), timeframe, prevState.Copy(), t1); err != nil {
					return err
				}
				runner.IncrementStatValue(STAT_VALUE_DATA_COUNT, int64(len(batch)))
			}

			if runner.MustInterrupt() {
				break
			}

			t0 = t1
			t1 = t1.Add(timeframe)
			runner.SetSize().Current(t0.Int(), false)
		}
		runner.AddStep()
		printTimeframeIndexingStatus(runner, asset)
		return nil
	}

	runner.AddProcess(process)
}

func getDepAddresses(address pcommon.AssetAddress) []pcommon.AssetAddress {
	ret := []pcommon.AssetAddress{}
	p, _ := address.Parse()
	for _, d := range p.Dependencies {
		ret = append(ret, d)
		ret = append(ret, getDepAddresses(d)...)
	}
	return lo.Uniq[pcommon.AssetAddress](ret)
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

func unpointerData(d pcommon.Data) pcommon.Data {
	value := reflect.ValueOf(d)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	// Ensure last is not a pointer
	return value.Interface().(pcommon.Data)
}
