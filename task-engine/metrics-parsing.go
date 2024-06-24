package engine

// import (
// 	"fmt"
// 	"os"
// 	"sync"

// 	"pendulev2/dtype"
// 	setlib "pendulev2/set2"

// 	"github.com/fantasim/gorunner"
// 	pcommon "github.com/pendulea/pendule-common"
// 	log "github.com/sirupsen/logrus"
// )

// const (
// 	METRICS_PARSING_KEY = "metrics_parsing"
// )

// func printMetricsParsingStatus(runner *gorunner.Runner, setID string) {
// 	date := getDate(runner)

// 	assetStateIDs := parseAssetStateIDs(runner)

// 	if runner.IsRunning() {
// 		total := runner.StatValue(STAT_VALUE_DATA_COUNT)

// 		log.WithFields(log.Fields{
// 			"rows": pcommon.Format.LargeNumberToShortString(total),
// 			"done": "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
// 		}).Info(fmt.Sprintf("Successfully stored %s %d columns (%s)", setID, len(assetStateIDs), date))
// 	}
// }

// func addMetricsParsingRunnerProcess(runner *gorunner.Runner, pair *pcommon.Pair) {

// 	process := func() error {
// 		date := getDate(runner)

// 		set := Engine.Sets.Find(pair.BuildSetID())
// 		if set == nil {
// 			return fmt.Errorf("set not found")
// 		}

// 		archiveFilePathCSV := pair.BuildFuturesMetricsArchivesFilePath(date, "csv")
// 		archiveFilePathZIP := pair.BuildFuturesMetricsArchivesFilePath(date, "zip")
// 		archiveFolderPath := pair.BuildFuturesMetricsArchiveFolderPath()

// 		defer func() {
// 			if os.Remove(archiveFilePathCSV) == nil {
// 				runner.SetStatValue("CSV_FILE_REMOVED", 1)
// 			}
// 		}()

// 		archiveZipSize, err := pcommon.File.GetFileSize(archiveFilePathZIP)
// 		if err != nil {
// 			return err
// 		}

// 		runner.SetStatValue(STAT_VALUE_ARCHIVE_SIZE, archiveZipSize)

// 		err = pcommon.File.UnzipFile(archiveFilePathZIP, archiveFolderPath)
// 		if err != nil {
// 			if err.Error() == "zip: not a valid zip file" {
// 				os.Remove(archiveFilePathZIP)
// 			}
// 			return err
// 		}

// 		metrics, err := pair.ParseMetricsFromCSV(date)
// 		if err != nil {
// 			return err
// 		}
// 		if len(metrics) == 0 {
// 			log.WithFields(log.Fields{
// 				"set": set.ID(),
// 			}).Warn("No metrics data found")
// 		}

// 		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(len(metrics)))

// 		wg := sync.WaitGroup{}
// 		wg.Add(5)

// 		stateMap := map[string]map[pcommon.TimeUnit][]byte{}
// 		mu := sync.RWMutex{}

// 		stateMap[setlib.METRIC_SUM_OPEN_INTEREST] = map[pcommon.TimeUnit][]byte{}
// 		stateMap[setlib.METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO] = map[pcommon.TimeUnit][]byte{}
// 		stateMap[setlib.METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO] = map[pcommon.TimeUnit][]byte{}
// 		stateMap[setlib.METRIC_COUNT_LONG_SHORT_RATIO] = map[pcommon.TimeUnit][]byte{}
// 		stateMap[setlib.METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO] = map[pcommon.TimeUnit][]byte{}

// 		setData := func(s *setlib.AssetState, data dtype.UnitTimeArray) {
// 			defer mu.Unlock()
// 			mu.Lock()
// 			stateMap[s.ID()] = data.ToRaw(s.Precision())
// 			wg.Done()
// 		}

// 		go func(stateID string) {
// 			res := dtype.UnitTimeArray{}
// 			for _, m := range metrics {
// 				timestamp := pcommon.NewTimeUnitFromTime(m.CreateTime)
// 				res = append(res, dtype.NewUnit(m.SumOpenInterest).ToTime(timestamp))
// 			}
// 			setData(set.Assets[stateID], res)
// 		}(setlib.METRIC_SUM_OPEN_INTEREST)

// 		go func(stateID string) {
// 			res := dtype.UnitTimeArray{}
// 			for _, m := range metrics {
// 				timestamp := pcommon.NewTimeUnitFromTime(m.CreateTime)
// 				res = append(res, dtype.NewUnit(m.CountTopTraderLongShortRatio).ToTime(timestamp))
// 			}
// 			setData(set.Assets[stateID], res)
// 		}(setlib.METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO)

// 		go func(stateID string) {
// 			res := dtype.UnitTimeArray{}
// 			for _, m := range metrics {
// 				timestamp := pcommon.NewTimeUnitFromTime(m.CreateTime)
// 				res = append(res, dtype.NewUnit(m.SumTopTraderLongShortRatio).ToTime(timestamp))
// 			}
// 			setData(set.Assets[stateID], res)
// 		}(setlib.METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO)

// 		go func(stateID string) {
// 			res := dtype.UnitTimeArray{}
// 			for _, m := range metrics {
// 				timestamp := pcommon.NewTimeUnitFromTime(m.CreateTime)
// 				res = append(res, dtype.NewUnit(m.CountLongShortRatio).ToTime(timestamp))
// 			}
// 			setData(set.Assets[stateID], res)
// 		}(setlib.METRIC_COUNT_LONG_SHORT_RATIO)

// 		go func(stateID string) {
// 			res := dtype.UnitTimeArray{}
// 			for _, m := range metrics {
// 				timestamp := pcommon.NewTimeUnitFromTime(m.CreateTime)
// 				res = append(res, dtype.NewUnit(m.SumTakerLongShortVolRatio).ToTime(timestamp))
// 			}
// 			setData(set.Assets[stateID], res)
// 		}(setlib.METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO)

// 		wg.Wait()
// 		wg.Add(5)
// 		for stateID, data := range stateMap {
// 			go func(state *setlib.AssetState, data map[pcommon.TimeUnit][]byte) {
// 				lerr := state.Store(data, pcommon.Env.MIN_TIME_FRAME, -1)
// 				if lerr != nil {
// 					mu.Lock()
// 					err = lerr
// 					mu.Unlock()
// 				}
// 				wg.Done()
// 			}(set.Assets[stateID], data)
// 		}
// 		wg.Wait()
// 		if err != nil {
// 			return err
// 		}

// 		if err := updateAllConsistencyTime(set, parseAssetStateIDs(runner), date); err != nil {
// 			return err
// 		}

// 		printMetricsParsingStatus(runner, set.ID())
// 		runner.AddStep()
// 		return nil
// 	}

// 	runner.AddProcess(process)
// }

// func buildMetricsParsingRunner(pair *pcommon.Pair, concernedAssets setlib.AssetStates, date string) *gorunner.Runner {
// 	runner := gorunner.NewRunner(METRICS_PARSING_KEY + "-" + pair.BuildSetID() + "-" + date)

// 	addTimeframe(runner, pcommon.Env.MIN_TIME_FRAME)
// 	addDate(runner, date)
// 	addAssetAndSetIDs(runner, concernedAssets.SetAndAssetIDs())

// 	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
// 		for _, r := range details.RunningRunners {

// 			if !haveSameFullIDs(r, runner) {
// 				continue
// 			}

// 			if !haveSameTimeframe(r, runner) {
// 				continue
// 			}

// 			return false
// 		}

// 		return true
// 	})

// 	addMetricsParsingRunnerProcess(runner, pair)
// 	return runner
// }
