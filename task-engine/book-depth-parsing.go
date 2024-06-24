package engine

// import (
// 	"fmt"
// 	"math"
// 	"os"
// 	"pendulev2/dtype"
// 	setlib "pendulev2/set2"
// 	"strconv"
// 	"sync"

// 	"github.com/fantasim/gorunner"
// 	pcommon "github.com/pendulea/pendule-common"
// 	log "github.com/sirupsen/logrus"
// )

// const (
// 	BOOKDEPTH_PARSING_KEY = "bookdepth_parsing"
// )

// func printBookDepthParsingStatus(runner *gorunner.Runner, setID string) {
// 	date := getDate(runner)

// 	assetStateIDs := parseAssetStateIDs(runner)

// 	if runner.IsRunning() {
// 		totalBookDepth := runner.StatValue(STAT_VALUE_DATA_COUNT)

// 		log.WithFields(log.Fields{
// 			"rows": pcommon.Format.LargeNumberToShortString(totalBookDepth),
// 			"done": "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
// 		}).Info(fmt.Sprintf("Successfully stored %s %d columns (%s)", setID, len(assetStateIDs), date))
// 	}
// }

// func addBookDepthRunnerProcess(runner *gorunner.Runner, pair *pcommon.Pair) {

// 	process := func() error {
// 		task := runner.Task
// 		date, _ := gorunner.GetArg[string](task.Args, ARG_VALUE_DATE)

// 		set := Engine.Sets.Find(pair.BuildSetID())
// 		if set == nil {
// 			return fmt.Errorf("set not found")
// 		}

// 		archiveFilePathCSV := pair.BuildBookDepthArchivesFilePath(date, "csv")
// 		archiveFilePathZIP := pair.BuildBookDepthArchivesFilePath(date, "zip")
// 		archiveFolderPath := pair.BuildBookDepthArchiveFolderPath()

// 		defer func() {
// 			if os.Remove(archiveFilePathCSV) == nil {
// 				task.SetStatValue("CSV_FILE_REMOVED", 1)
// 			}
// 		}()

// 		archiveZipSize, err := pcommon.File.GetFileSize(archiveFilePathZIP)
// 		if err != nil {
// 			return err
// 		}

// 		task.SetStatValue(STAT_VALUE_ARCHIVE_SIZE, archiveZipSize)

// 		err = pcommon.File.UnzipFile(archiveFilePathZIP, archiveFolderPath)
// 		if err != nil {
// 			if err.Error() == "zip: not a valid zip file" {
// 				os.Remove(archiveFilePathZIP)
// 			}
// 			return err
// 		}

// 		list, err := pair.ParseBookDepthFromCSV(date)
// 		if err != nil {
// 			return err
// 		}
// 		if len(list) == 0 {
// 			log.WithFields(log.Fields{
// 				"set": set.ID(),
// 			}).Warn("No book depth data found")
// 		}
// 		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(len(list))/10)

// 		m := make(map[int]dtype.UnitTimeArray)
// 		for percent := -5; percent <= 5; percent++ {
// 			if percent != 0 {
// 				m[percent] = dtype.UnitTimeArray{}
// 			}
// 		}

// 		for _, bd := range list {
// 			timestamp := pcommon.NewTimeUnitFromTime(bd.Timestamp)
// 			m[bd.Percentage] = append(m[bd.Percentage], dtype.NewUnit(bd.Depth).ToTime(timestamp))
// 		}

// 		wg := sync.WaitGroup{}
// 		wg.Add(10)
// 		mu := sync.RWMutex{}
// 		err = nil

// 		for percent := -5; percent <= 5; percent++ {
// 			if percent != 0 {
// 				sign := pcommon.When[string](percent < 0).Then("m").Else("p")
// 				stateID := "bd-" + sign + strconv.Itoa(int(math.Abs(float64(percent))))
// 				if set.Assets[stateID] == nil {
// 					return fmt.Errorf("asset %s not found", stateID)
// 				}
// 				go func(state *setlib.AssetState, data dtype.UnitTimeArray) {
// 					lerr := state.Store(data.ToRaw(state.Precision()), pcommon.Env.MIN_TIME_FRAME, -1)
// 					if lerr != nil {
// 						mu.Lock()
// 						err = lerr
// 						mu.Unlock()
// 					}
// 					wg.Done()
// 				}(set.Assets[stateID], m[percent])
// 			}
// 		}
// 		wg.Wait()

// 		if err != nil {
// 			return err
// 		}

// 		if err := updateAllConsistencyTime(set, parseAssetStateIDs(runner), date); err != nil {
// 			return err
// 		}

// 		printBookDepthParsingStatus(runner, set.ID())
// 		runner.AddStep()
// 		return nil
// 	}
// 	runner.AddProcess(process)
// }

// func buildBookDepthParsingRunner(pair *pcommon.Pair, concernedAssets setlib.AssetStates, date string) *gorunner.Runner {

// 	runner := gorunner.NewRunner(BOOKDEPTH_PARSING_KEY + "-" + concernedAssets[0].SetRef.ID() + "-" + date)

// 	concernedAssets.SetAndAssetIDs()

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

// 	addBookDepthRunnerProcess(runner, pair)
// 	return runner
// }
