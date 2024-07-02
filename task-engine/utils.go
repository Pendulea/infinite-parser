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
	ARG_VALUE_ADDRESSES = "addresses"
	ARG_VALUE_TIMEFRAME = "timeframe"
)

func addDate(r *gorunner.Runner, date string) {
	r.Args[ARG_VALUE_DATE] = date
}

func addTimeframe(r *gorunner.Runner, timeframe time.Duration) {
	r.Args[ARG_VALUE_TIMEFRAME] = timeframe
}

func addAssetAddresses(r *gorunner.Runner, addresses []pcommon.AssetAddress) {
	r.Args[ARG_VALUE_ADDRESSES] = addresses
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

func haveSameAddresses(r1, r2 *gorunner.Runner) bool {
	addresses1, ok := gorunner.GetArg[[]pcommon.AssetAddress](r1.Args, ARG_VALUE_ADDRESSES)
	addresses2, ok2 := gorunner.GetArg[[]pcommon.AssetAddress](r2.Args, ARG_VALUE_ADDRESSES)

	if !ok || !ok2 {
		return false
	}

	for _, address1 := range addresses1 {
		for _, address2 := range addresses2 {
			if address1 == address2 {
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
