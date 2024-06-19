package engine

import (
	"errors"
	"log"
	"os"
	setlib "pendulev2/set2"
	"strings"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
)

// import (
// 	"pendule/indicator"
// 	"strings"
// 	"time"

// 	gorunner "github.com/fantasim/gorunner"
// 	pcommon "github.com/pendulea/pendule-common"
// )

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

func haveSameSetID(r1, r2 *gorunner.Runner) bool {
	setID1, ok := gorunner.GetArg[string](r1.Args, ARG_VALUE_SET_ID)
	setID2, ok2 := gorunner.GetArg[string](r2.Args, ARG_VALUE_SET_ID)

	if !ok || !ok2 {
		return false
	}

	return setID1 == setID2
}

func haveSameTimeframe(r1, r2 *gorunner.Runner) bool {
	timeframe1, ok := gorunner.GetArg[time.Duration](r1.Args, ARG_VALUE_TIMEFRAME)
	timeframe2, ok2 := gorunner.GetArg[time.Duration](r2.Args, ARG_VALUE_TIMEFRAME)

	if !ok || !ok2 {
		return false
	}

	return timeframe1 == timeframe2
}

func haveCommonAssets(r1, r2 *gorunner.Runner) bool {
	assets1, ok := gorunner.GetArg[string](r1.Args, ARG_VALUE_ASSETS)
	assets2, ok2 := gorunner.GetArg[string](r2.Args, ARG_VALUE_ASSETS)

	if !ok || !ok2 {
		return false
	}

	assets1Array := strings.Split(assets1, ",")
	assets2Array := strings.Split(assets2, ",")

	for _, a1 := range assets1Array {
		for _, a2 := range assets2Array {
			if a1 == a2 {
				return true
			}
		}
	}
	return false
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

func (e *engine) generalDataParsingCheck(pair *pcommon.Pair, concernedStates setlib.AssetStates, buildZipArchive func(string, string) string) (string, error) {
	set := e.Sets.Find(pair.BuildSetID())
	if set == nil {
		return "", errors.New("set not found")
	}

	leastConsistent, err := concernedStates.LeastConsistent(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return "", err
	}
	date, err := leastConsistent.Sync()
	if err != nil {
		return "", err
	}
	if date == nil {
		return "", errors.New("already sync")
	}

	info, err := os.Stat(buildZipArchive(*date, "zip"))
	if err != nil {
		return "", err
	}
	if info.ModTime().Unix() > time.Now().Add(-time.Minute).Unix() {
		return "", errors.New("file is too recent")
	}

	return *date, nil
}
