package engine

import (
	"log"
	"os"
	"strings"
	"time"

	pcommon "github.com/pendulea/pendule-common"

	"github.com/fantasim/gorunner"
)

const (
	ARG_VALUE_DATE      = "date"
	ARG_VALUE_FULL_IDS  = "full_ids"
	ARG_VALUE_TIMEFRAME = "timeframe"
)

func addDate(r *gorunner.Runner, date string) {
	r.Args[ARG_VALUE_DATE] = date
}

func addTimeframe(r *gorunner.Runner, timeframe time.Duration) {
	r.Args[ARG_VALUE_TIMEFRAME] = timeframe
}

func addAssetAndSetIDs(r *gorunner.Runner, fullIDs []string) {
	r.Args[ARG_VALUE_FULL_IDS] = strings.Join(fullIDs, ",")
}

func getDate(r *gorunner.Runner) string {
	date, ok := gorunner.GetArg[string](r.Args, ARG_VALUE_DATE)
	if !ok {
		log.Fatal("Date not found in runner")
	}
	return date
}

func getTimeframe(r *gorunner.Runner) time.Duration {
	timeframe, ok := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
	if !ok {
		log.Fatal("Timeframe not found in runner")
	}
	return timeframe
}

func parseAssetStateIDs(r *gorunner.Runner) []string {
	fullIDs, ok := gorunner.GetArg[string](r.Args, ARG_VALUE_FULL_IDS)
	if !ok {
		log.Fatal("Full IDs not found in runner")
	}
	array := strings.Split(fullIDs, ",")
	ret := make([]string, len(array))
	for i, fullID := range array {
		sp := strings.Split(fullID, ":")
		ret[i] = sp[1]
	}
	return ret
}

func haveSameFullIDs(r1, r2 *gorunner.Runner) bool {
	setID1, ok := gorunner.GetArg[string](r1.Args, ARG_VALUE_FULL_IDS)
	setID2, ok2 := gorunner.GetArg[string](r2.Args, ARG_VALUE_FULL_IDS)

	if !ok || !ok2 {
		return false
	}

	setID1Array := strings.Split(setID1, ",")
	setID2Array := strings.Split(setID2, ",")
	for _, s1 := range setID1Array {
		for _, s2 := range setID2Array {
			if s1 == s2 {
				return true
			}
		}
	}
	return false
}

func haveSameTimeframe(r1, r2 *gorunner.Runner) bool {
	timeframe1, ok := gorunner.GetArg[time.Duration](r1.Args, ARG_VALUE_TIMEFRAME)
	timeframe2, ok2 := gorunner.GetArg[time.Duration](r2.Args, ARG_VALUE_TIMEFRAME)

	if !ok || !ok2 {
		return false
	}

	return timeframe1 == timeframe2
}

// func isRunnerTimeframeDeletion(runner *gorunner.Runner) bool {
// 	return strings.Contains(runner.ID, TIMEFRAME_DELETION_KEY)
// }

// func isRunnerTimeframeIndexing(runner *gorunner.Runner) bool {
// 	return strings.Contains(runner.ID, TIMEFRAME_INDEXING_KEY)
// }

// func isRunnerIndicatorIndexing(runner *gorunner.Runner) bool {
// 	return strings.Contains(runner.ID, INDICATOR_INDEXING_RUNNER)
// }

// func isRunnerIndicatorDeletion(runner *gorunner.Runner) bool {
// 	return strings.Contains(runner.ID, INDICATOR_DELETION_KEY)
// }

// func isRunnerTradeParsing(runner *gorunner.Runner) bool {
// 	return strings.Contains(runner.ID, TRADE_PARSING_KEY)
// }

// func isRunnerCSVBuilding(runner *gorunner.Runner) bool {
// 	return strings.Contains(runner.ID, CSV_BUILDING_KEY)
// }

// type StatusHTML struct {
// 	SetID string `json:"set_id"`
// 	HTML  string `json:"html"`
// }

// func HTMLify(r *gorunner.Runner) StatusHTML {
// 	html := StatusHTML{}
// 	setID, _ := gorunner.GetArg[string](r.Args, ARG_VALUE_SET_ID)
// 	html.SetID = setID

// 	if strings.Contains(r.ID, TIMEFRAME_INDEXING_KEY) {
// 		timeframe, _ := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
// 		label, _ := pcommon.Format.TimeFrameToLabel(timeframe)

// 		ETAString := pcommon.Format.AccurateHumanize(r.ETA())

// 		html.HTML = "<span>Indexing " + "<span style=\"font-weight: 700\">" + label + "</span>" + " candles " + "(<span style=\"font-weight: 700; color: green;\">" + ETAString + "</span>)" + "</span>"
// 	}
// 	if strings.Contains(r.ID, INDICATOR_INDEXING_RUNNER) {
// 		indicator, _ := gorunner.GetArg[indicator.Indicator](r.Args, ARG_VALUE_INDICATOR)
// 		timeframe, _ := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
// 		label, _ := pcommon.Format.TimeFrameToLabel(timeframe)

// 		ETAString := pcommon.Format.AccurateHumanize(r.ETA())

// 		html.HTML = "<span>Indexing " + "<span style=\"font-weight: 700\">" + string(indicator) + "</span>" + " on " + "<span style=\"font-weight: 700; color: green;\">" + label + "</span>" + " candles " + "(<span style=\"font-weight: 700\">" + ETAString + "</span>)" + "</span>"
// 	}
// 	if strings.Contains(r.ID, CSV_BUILDING_KEY) {
// 		timeframe, _ := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
// 		label, _ := pcommon.Format.TimeFrameToLabel(timeframe)

// 		ETAString := pcommon.Format.AccurateHumanize(r.ETA())

// 		html.HTML = "<span>Building CSV on " + "<span style=\"font-weight: 700\">" + label + "</span>" + " candles " + "(<span style=\"font-weight: 700; color: green;\">" + ETAString + "</span>)" + "</span>"
// 	}
// 	if strings.Contains(r.ID, CANDLE_PARSING_KEY) {
// 		ETAString := pcommon.Format.AccurateHumanize(r.ETA())
// 		html.HTML = "<span>Indexing " + "<span style=\"font-weight: 700\">" + "1s" + "</span>" + " candles " + "(<span style=\"font-weight: 700; color: green;\">" + ETAString + "</span>)" + "</span>"
// 	}

// 	return html
// }

// func getPrecision(val float64) int {
// 	// Convert the float64 to a string with high precision
// 	str := pcommon.Format.Float(val, -1)
// 	// Find the position of the decimal point
// 	decimalPos := strings.Index(str, ".")
// 	if decimalPos == -1 {
// 		// No decimal point found, so the precision is 0
// 		return 0
// 	}
// 	// The precision is the number of characters after the decimal point
// 	return len(str) - decimalPos - 1
// }

// func (e *engine) generalStateParsing(concernedStates setlib.AssetStates) (string, error) {
// 	leastConsistent, err := concernedStates.LeastConsistent(pcommon.Env.MIN_TIME_FRAME)
// 	if err != nil {
// 		return "", err
// 	}
// 	date, err := leastConsistent.ShouldSync()
// 	if err != nil {
// 		return "", err
// 	}
// 	if date == nil {
// 		return "", errors.New("already sync")
// 	}

// 	info, err := os.Stat(buildZipArchive(*date, "zip"))
// 	if err != nil {
// 		return "", err
// 	}
// 	if info.ModTime().Unix() > time.Now().Add(-time.Minute).Unix() {
// 		return "", errors.New("file is too recent")
// 	}

// 	return *date, nil
// }

// func updateAllConsistencyTime(set *setlib.Set, stateIDs []string, date string) error {
// 	dateTime, err := pcommon.Format.StrDateToDate(date)
// 	if err != nil {
// 		return err
// 	}

// 	tx := set.Assets[stateIDs[0]].NewTX(true)
// 	for _, id := range stateIDs {
// 		if err := set.Assets[id].
// 			SetNewConsistencyTime(
// 				pcommon.Env.MIN_TIME_FRAME,
// 				pcommon.NewTimeUnitFromTime(dateTime.Add(time.Hour*24)),
// 				tx); err != nil {
// 			return err
// 		}
// 	}
// 	if err := tx.Commit(); err != nil {
// 		return err
// 	}

// 	return nil
// }

func GetCSVList() ([]pcommon.CSVStatus, error) {
	list, err := pcommon.File.GetSortedFilenamesByDate(os.Getenv("CSV_DIR"))
	if err != nil {
		return nil, err
	}
	statuses := []pcommon.CSVStatus{}
	used := map[string]bool{}

	for _, runner := range Engine.RunningRunners() {
		if strings.Contains(runner.ID, CSV_BUILDING_KEY) {
			parameters := getParameters(runner)
			status := parameters.Status(runner)
			used[status.BuildID] = true
			statuses = append(statuses, status)
		}
	}

	for _, file := range list {
		//check if file ends with zip
		if strings.HasSuffix(file.Name, ".zip") {
			buildID := strings.ReplaceAll(file.Name, ".zip", "")
			if _, ok := used[buildID]; ok {
				continue
			}
			statuses = append(statuses, CSVBuildingOrderIDToStatus(buildID, file))
		}
	}
	return statuses, nil
}

func HTMLify(r *gorunner.Runner) pcommon.StatusHTML {
	html := pcommon.StatusHTML{}
	// setID, _ := gorunner.GetArg[string](r.Args, ARG_VALUE_SET_ID)
	// html.SetID = setID

	// if strings.Contains(r.ID, TIMEFRAME_INDEXING_KEY) {
	// 	timeframe, _ := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
	// 	label, _ := pcommon.Format.TimeFrameToLabel(timeframe)

	// 	ETAString := pcommon.Format.AccurateHumanize(r.ETA())

	// 	html.HTML = "<span>Indexing " + "<span style=\"font-weight: 700\">" + label + "</span>" + " candles " + "(<span style=\"font-weight: 700; color: green;\">" + ETAString + "</span>)" + "</span>"
	// }
	// if strings.Contains(r.ID, INDICATOR_INDEXING_RUNNER) {
	// 	indicator, _ := gorunner.GetArg[indicator.Indicator](r.Args, ARG_VALUE_INDICATOR)
	// 	timeframe, _ := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
	// 	label, _ := pcommon.Format.TimeFrameToLabel(timeframe)

	// 	ETAString := pcommon.Format.AccurateHumanize(r.ETA())

	// 	html.HTML = "<span>Indexing " + "<span style=\"font-weight: 700\">" + string(indicator) + "</span>" + " on " + "<span style=\"font-weight: 700; color: green;\">" + label + "</span>" + " candles " + "(<span style=\"font-weight: 700\">" + ETAString + "</span>)" + "</span>"
	// }
	// if strings.Contains(r.ID, CSV_BUILDING_KEY) {
	// 	timeframe, _ := gorunner.GetArg[time.Duration](r.Args, ARG_VALUE_TIMEFRAME)
	// 	label, _ := pcommon.Format.TimeFrameToLabel(timeframe)

	// 	ETAString := pcommon.Format.AccurateHumanize(r.ETA())

	// 	html.HTML = "<span>Building CSV on " + "<span style=\"font-weight: 700\">" + label + "</span>" + " candles " + "(<span style=\"font-weight: 700; color: green;\">" + ETAString + "</span>)" + "</span>"
	// }
	// if strings.Contains(r.ID, CANDLE_PARSING_KEY) {
	// 	ETAString := pcommon.Format.AccurateHumanize(r.ETA())
	// 	html.HTML = "<span>Indexing " + "<span style=\"font-weight: 700\">" + "1s" + "</span>" + " candles " + "(<span style=\"font-weight: 700; color: green;\">" + ETAString + "</span>)" + "</span>"
	// }

	return html
}
