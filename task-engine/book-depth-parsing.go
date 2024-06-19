package engine

import (
	"fmt"
	"math"
	"os"
	setlib "pendulev2/set2"
	"strconv"
	"strings"
	"sync"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	BOOKDEPTH_PARSING_KEY = "bookdepth_parsing"
)

func printBookDepthParsingStatus(runner *gorunner.Runner, setID string) {
	date := getDate(runner)

	assets, _ := gorunner.GetArg[string](runner.Args, ARG_VALUE_ASSETS)

	if runner.IsRunning() {
		totalBookDepth := runner.StatValue(STAT_VALUE_DATA_COUNT)

		log.WithFields(log.Fields{
			"rows": pcommon.Format.LargeNumberToShortString(totalBookDepth),
			"done": "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
		}).Info(fmt.Sprintf("Successfully stored %s %s rows (%s)", setID, assets, date))
	}
}

func addBookDepthRunnerProcess(runner *gorunner.Runner, pair *pcommon.Pair) {

	process := func() error {
		task := runner.Task
		date, _ := gorunner.GetArg[string](task.Args, ARG_VALUE_DATE)

		set := Engine.Sets.Find(pair.BuildSetID())
		if set == nil {
			return fmt.Errorf("set not found")
		}

		archiveFilePathCSV := pair.BuildBookDepthArchivesFilePath(date, "csv")
		archiveFilePathZIP := pair.BuildBookDepthArchivesFilePath(date, "zip")
		archiveFolderPath := pair.BuildBookDepthArchiveFolderPath()

		defer func() {
			if os.Remove(archiveFilePathCSV) == nil {
				task.SetStatValue("CSV_FILE_REMOVED", 1)
			}
		}()

		archiveZipSize, err := pcommon.File.GetFileSize(archiveFilePathZIP)
		if err != nil {
			return err
		}

		task.SetStatValue(STAT_VALUE_ARCHIVE_SIZE, archiveZipSize)

		err = pcommon.File.UnzipFile(archiveFilePathZIP, archiveFolderPath)
		if err != nil {
			if err.Error() == "zip: not a valid zip file" {
				os.Remove(archiveFilePathZIP)
			}
			return err
		}

		list, err := pair.ParseBookDepthFromCSV(date)
		if err != nil {
			return err
		}
		if len(list) == 0 {
			log.WithFields(log.Fields{
				"set": set.ID(),
			}).Warn("No book depth data found")
		}
		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(len(list))/10)

		m := make(map[int]setlib.UnitTimeArray)
		for percent := -5; percent <= 5; percent++ {
			if percent != 0 {
				m[percent] = setlib.UnitTimeArray{}
			}
		}

		for _, bd := range list {
			timestamp := pcommon.NewTimeUnitFromTime(bd.Timestamp)
			m[bd.Percentage] = append(m[bd.Percentage], setlib.NewUnit(bd.Depth).ToTime(timestamp))
		}

		wg := sync.WaitGroup{}
		wg.Add(10)
		mu := sync.RWMutex{}
		err = nil

		for percent := -5; percent <= 5; percent++ {
			if percent != 0 {
				sign := pcommon.When[string](percent < 0).Then("m").Else("p")
				stateID := "bd-" + sign + strconv.Itoa(int(math.Abs(float64(percent))))
				if set.Assets[stateID] == nil {
					return fmt.Errorf("asset %s not found", stateID)
				}
				go func(state *setlib.AssetState, data setlib.UnitTimeArray) {
					lerr := state.Store(data.ToRaw(state.Precision()), pcommon.Env.MIN_TIME_FRAME, -1)
					if lerr != nil {
						mu.Lock()
						err = lerr
						mu.Unlock()
					}
					wg.Done()
				}(set.Assets[stateID], m[percent])
			}
		}
		wg.Wait()

		if err != nil {
			return err
		}

		if err := updateAllConsistencyTime(set, getAssets(runner), date); err != nil {
			return err
		}

		printBookDepthParsingStatus(runner, set.ID())
		runner.AddStep()
		return nil
	}
	runner.AddProcess(process)
}

func buildBookDepthParsingRunner(pair *pcommon.Pair, date string) *gorunner.Runner {
	runner := gorunner.NewRunner(BOOKDEPTH_PARSING_KEY + "-" + pair.BuildSetID() + "-" + date)

	concernedAssets := []string{setlib.BOOK_DEPTH_M1, setlib.BOOK_DEPTH_M2, setlib.BOOK_DEPTH_M3, setlib.BOOK_DEPTH_M4, setlib.BOOK_DEPTH_M5, setlib.BOOK_DEPTH_P1, setlib.BOOK_DEPTH_P2, setlib.BOOK_DEPTH_P3, setlib.BOOK_DEPTH_P4, setlib.BOOK_DEPTH_P5}

	runner.AddArgs(ARG_VALUE_TIMEFRAME, pcommon.Env.MIN_TIME_FRAME)
	runner.AddArgs(ARG_VALUE_DATE, date)
	runner.AddArgs(ARG_VALUE_SET_ID, pair.BuildSetID())
	runner.AddArgs(ARG_VALUE_ASSETS, strings.Join(concernedAssets, ","))

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

	addBookDepthRunnerProcess(runner, pair)
	return runner
}
