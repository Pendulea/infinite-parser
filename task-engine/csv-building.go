package engine

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	setlib "pendulev2/set2"
	"pendulev2/util"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
)

const MAX_SIZE_CSV_ARCHIVE_BYTES int64 = 3 * 1024 * 1024 * 1024 // 3GB
const MAX_SIZE_CSV_FILE_BYTES int64 = 100 * 1024 * 1024         // 100MB
const MAX_CANDLE_PER_BATCH = 10_000

const (
	CSV_BUILDING_KEY      = "csv_building"
	ARG_VALUE_PARAMETERS  = "parameters"
	STAT_VALUE_LINE_COUNT = "LINE_COUNT"
)

func runnerStatusString(runner *gorunner.Runner) string {
	if !runner.HasStarted() {
		return "SCHEDULED"
	} else if !runner.IsDone() {
		switch runner.CountSteps() {
		case 0:
			return "WRITING"
		case 1:
			return "ZIPPING"
		}
	}
	return "DONE"
}

func GetCSVStatus(runner *gorunner.Runner) pcommon.CSVStatus {
	parameters := getCSVParameters(runner)
	buildID := parameters.Orders.BuildID(parameters.Header)

	status := pcommon.CSVStatus{
		BuildID: buildID,
	}

	status.Percent = runner.Percent()
	status.Status = runnerStatusString(runner)
	status.Size = runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)
	status.From = parameters.Header.From
	status.To = parameters.Header.To
	status.TimeframeLabel, _ = pcommon.Format.TimeFrameToLabel(parameters.Header.Timeframe)
	status.Assets = lo.Map(parameters.Orders, func(order setlib.CSVAsset, idx int) []string {
		list := []string{string(order.Asset.Address())}
		list = append(list, util.ColumnNamesToStrings(order.Columns.Columns())...)
		return list
	})
	return status
}

func printBuildCSVStatus(runner *gorunner.Runner) {
	csvStatus := GetCSVStatus(runner)

	switch runner.CountSteps() {
	case 0:
		log.WithFields(log.Fields{
			"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
			"buildID":  csvStatus.BuildID,
			"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
		}).Info("Building CSV archive")
	case 1:
		log.WithFields(log.Fields{
			"size":    pcommon.Format.LargeBytesToShortString(runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)),
			"buildID": csvStatus.BuildID,
		}).Info("Zipping CSV archive")
	case 2:
		log.WithFields(log.Fields{
			"size":    pcommon.Format.LargeBytesToShortString(runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)),
			"buildID": csvStatus.BuildID,
			"done":    "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
		}).Info("Successfully built CSV")
	}
}

func buildQuerySummaryFile(runner *gorunner.Runner) error {
	parameters := getCSVParameters(runner)
	status := GetCSVStatus(runner)

	lineWritten := runner.StatValue(STAT_VALUE_LINE_COUNT)

	setIDs := lo.Uniq(lo.Map(parameters.Orders, func(order setlib.CSVAsset, idx int) string {
		return order.Asset.SetRef.ID()
	}))

	countSetIDs := len(setIDs)

	content := fmt.Sprintf("Timeframe: %s\nBetween: %s 00:00:00 and %s\n\n", status.TimeframeLabel,
		parameters.Header.From.ToTime().UTC().Format("2006-01-02"),
		parameters.Header.To.ToTime().UTC().Format("2006-01-02 15:04:05"))

	for _, setID := range setIDs {
		listFullColumns := []string{}
		for _, order := range parameters.Orders {
			if order.Asset.SetRef.ID() == setID {
				id, err := order.Asset.ParsedAddress().BuildCSVColumnName(countSetIDs > 1)
				if err != nil {
					return err
				}
				listFullColumns = append(listFullColumns, "- "+id+" : "+strings.Join(util.ColumnNamesToStrings(order.Columns.Columns()), ", "))
			}
		}
		content += fmt.Sprintf("%s:\n%s\n", setID, strings.Join(listFullColumns, "\n"))
	}
	content += fmt.Sprintf("\n\nTotal lines: %s\n", pcommon.Format.LargeNumberToShortString(lineWritten))

	queryFile := filepath.Join(parameters.BuildCSVArchiveFolderPath(), "query.txt")
	return util.WriteToFile(queryFile, content)
}

func buildCSV(runner *gorunner.Runner) error {
	parameters := getCSVParameters(runner)
	runner.SetSize().Initial(parameters.Header.From.Int())
	runner.SetSize().Max(parameters.Header.To.Int())

	header, err := parameters.BuildCSVHeader()
	if err != nil {
		return err
	}
	froms := parameters.BuildOrderFromTimes()

	go monitorProgress(runner)

	file, writer, err := createCSVFile(parameters.BuildCSVArchiveFolderPath(), 0, header)
	if err != nil {
		return err
	}
	defer closeCSVFile(writer, file)

	for turn, fileCount, cumulatedWrittenSize := 0, 0, int64(0); ; turn++ {
		listData, stopErr := parameters.FetchOrderData(&froms)
		if stopErr != nil {
			return stopErr
		}

		lines := collectLines(parameters, &listData)
		if len(lines) == 0 {
			break
		}

		if err := writeCSVLines(writer, lines, &froms, &cumulatedWrittenSize, runner); err != nil {
			return err
		}

		if cumulatedWrittenSize > MAX_SIZE_CSV_FILE_BYTES {
			if err := closeCSVFile(writer, file); err != nil {
				return err
			}
			fileCount++
			file, writer, err = createCSVFile(parameters.BuildCSVArchiveFolderPath(), fileCount, header)
			if err != nil {
				return err
			}
			cumulatedWrittenSize = 0
		}
	}

	if closeCSVFile(writer, file); err != nil {
		return err
	}

	runner.AddStep()
	if err := buildQuerySummaryFile(runner); err != nil {
		return err
	}
	if err := parameters.ZipCSVArchive(); err != nil {
		return err
	}
	runner.AddStep()
	go os.RemoveAll(parameters.BuildCSVArchiveFolderPath())
	printBuildCSVStatus(runner)
	return nil
}

func monitorProgress(runner *gorunner.Runner) {
	time.Sleep(2 * time.Second)
	for runner.Task.IsRunning() {
		printBuildCSVStatus(runner)
		time.Sleep(5 * time.Second)
	}
}

func createCSVFile(folderPath string, fileCount int, header []string) (*os.File, *csv.Writer, error) {
	csvFilePath := filepath.Join(folderPath, fmt.Sprintf("%d.csv", fileCount))
	file, err := os.Create(csvFilePath)
	if err != nil {
		return nil, nil, err
	}
	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		return nil, nil, err
	}
	return file, writer, nil
}

func closeCSVFile(writer *csv.Writer, file *os.File) error {
	writer.Flush()
	return file.Close()
}

func collectLines(parameters *setlib.CSVOrderUnpacked, listData *map[pcommon.AssetAddress]pcommon.DataList) []string {
	var lines []string

	for {
		minTime, minTimeState := findMinTimeState(parameters, listData)
		if minTime > parameters.Header.To || minTimeState == "" {
			break
		}

		line := createCSVLine(parameters, listData, minTime)
		lines = append(lines, strings.Join(line, ","))
	}

	return lines
}

func findMinTimeState(parameters *setlib.CSVOrderUnpacked, listData *map[pcommon.AssetAddress]pcommon.DataList) (pcommon.TimeUnit, pcommon.AssetAddress) {
	minTime := pcommon.NewTimeUnitFromTime(time.Now())
	var minTimeState pcommon.AssetAddress

	for _, order := range parameters.Orders {
		list := (*listData)[order.Asset.Address()]
		if list == nil || list.Len() == 0 {
			continue
		}
		first := list.First()
		if first.GetTime() < minTime {
			minTime, minTimeState = first.GetTime(), order.Asset.Address()
		}
	}
	return minTime, minTimeState
}

func areAllOrderDone(parameters *setlib.CSVOrderUnpacked, listData *map[pcommon.AssetAddress]pcommon.DataList) bool {
	doneCount := 0
	for _, order := range parameters.Orders {
		assetStateID := order.Asset.Address()
		list := (*listData)[assetStateID]
		if list == nil || list.Len() == 0 {
			doneCount++
		}
	}
	return doneCount == len(parameters.Orders)
}

func createCSVLine(parameters *setlib.CSVOrderUnpacked, listData *map[pcommon.AssetAddress]pcommon.DataList, minTime pcommon.TimeUnit) []string {
	var line []string

	for _, order := range parameters.Orders {
		precision := order.Asset.Decimals()
		columns := order.Columns

		assetStateID := order.Asset.Address()
		list := (*listData)[assetStateID]

		if list == nil || list.Len() == 0 {
			if !areAllOrderDone(parameters, listData) {
				assetLine := pcommon.NewTypeTime(order.Asset.DataType(), 0, 0).CSVLine(precision, columns)
				line = append(line, assetLine...)
			}
			continue
		}

		first := list.First()
		var assetLine []string
		if first.GetTime() == minTime {
			assetLine = first.CSVLine(precision, columns)
			(*listData)[assetStateID] = (*listData)[assetStateID].RemoveFirstN(1)
		} else {
			assetLine = pcommon.NewTypeTime(order.Asset.DataType(), 0, 0).CSVLine(precision, columns)
		}
		line = append(line, assetLine...)
	}
	return line
}

func writeCSVLines(writer *csv.Writer, lines []string, froms *[]pcommon.TimeUnit, cumulatedWrittenSize *int64, runner *gorunner.Runner) error {
	linesSize := 0
	for _, line := range lines {
		linesSize += len(line) + 1
		if err := writer.Write(strings.Split(line, ",")); err != nil {
			return err
		}
	}
	*cumulatedWrittenSize += int64(linesSize)
	runner.SetSize().Current(getLeastFromTime(*froms).Int(), false)
	runner.IncrementStatValue(STAT_VALUE_ARCHIVE_SIZE, int64(linesSize))
	runner.IncrementStatValue(STAT_VALUE_LINE_COUNT, int64(len(lines)))
	return nil
}

func getLeastFromTime(froms []pcommon.TimeUnit) pcommon.TimeUnit {
	min := pcommon.TimeUnit(math.MaxInt64)
	for _, from := range froms {
		if from < min {
			min = from
		}
	}
	return min
}

func buildCSVBuildingRunner(parameters *setlib.CSVOrderUnpacked) *gorunner.Runner {
	buildID := CSV_BUILDING_KEY + "-" + parameters.Orders.BuildID(parameters.Header)
	runner := gorunner.NewRunner(buildID)

	addresses := lo.Map(parameters.Orders, func(order setlib.CSVAsset, idx int) pcommon.AssetAddress {
		return order.Asset.Address()
	})

	addAssetAddresses(runner, addresses)
	addTimeframe(runner, parameters.Header.Timeframe)
	addCSVParameters(runner, parameters)

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

	runner.AddProcess(func() error {
		return buildCSV(runner)
	})

	return runner
}
