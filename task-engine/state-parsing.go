package engine

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	setlib "pendulev2/set2"
	"strconv"
	"strings"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	STATE_PARSING_KEY       = "state_parsing"
	STAT_VALUE_ARCHIVE_SIZE = "ARCHIVE_SIZE"
	STAT_VALUE_DATA_COUNT   = "DATA_COUNT"
)

func printStateParsingStatus(runner *gorunner.Runner, asset *setlib.AssetState) {
	date := getDate(runner)

	id, _ := asset.ParsedAddress().BuildCSVColumnName(true)
	if runner.IsRunning() {
		if runner.CountSteps() == 0 {
			archiveSize := runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)

			log.WithFields(log.Fields{
				"size": pcommon.Format.LargeBytesToShortString(archiveSize),
			}).Info(fmt.Sprintf("Unzipping %s archive (%s)", id, date))
			return
		} else if runner.CountSteps() == 1 {
			archiveSize := runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)

			log.WithFields(log.Fields{
				"size": pcommon.Format.LargeBytesToShortString(int64(float64(archiveSize) * 5.133)),
			}).Info(fmt.Sprintf("Parsing %s (%s)", id, date))

		} else if runner.CountSteps() == 2 {
			tradeParsed := pcommon.Format.LargeNumberToShortString(runner.Size().Current()) + "/" + pcommon.Format.LargeNumberToShortString(runner.Size().Max())

			log.WithFields(log.Fields{
				"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
				"speed":    pcommon.Format.LargeNumberToShortString(int64(runner.SizePerMillisecond()*1000)) + " line/s",
				"total":    tradeParsed,
				"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
			}).Info(fmt.Sprintf("Building %s (%s)", id, date))
		} else if runner.CountSteps() == 3 {
			totalRows := runner.StatValue(STAT_VALUE_DATA_COUNT)
			log.WithFields(log.Fields{
				"aggregated": pcommon.Format.LargeNumberToShortString(totalRows),
				"parsed":     pcommon.Format.LargeNumberToShortString(runner.Size().Max()),
			}).Info(fmt.Sprintf("Storing %s (%s)", id, date))

		} else if runner.CountSteps() >= 4 {
			totalRows := runner.StatValue(STAT_VALUE_DATA_COUNT)

			log.WithFields(log.Fields{
				"aggregated": pcommon.Format.LargeNumberToShortString(totalRows),
				"parsed":     pcommon.Format.LargeNumberToShortString(runner.Size().Max()),
				"done":       "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
			}).Info(fmt.Sprintf("Successfully stored %s (%s)", id, date))
		}
	}
}

func addStateParsingRunnerProcess(runner *gorunner.Runner, asset *setlib.AssetState) {
	process := func() error {

		timeframe := getTimeframe(runner)

		if asset.ParsedAddress().HasDependencies() {
			return nil
		}

		date := getDate(runner)
		dateToSync, err := asset.ShouldSync()
		if err != nil {
			return err
		}
		if *dateToSync != date {
			return nil
		}

		dateTime, err := pcommon.Format.StrDateToDate(date)
		if err != nil {
			return err
		}

		prevState, err := asset.GetLastPrevStateCached(timeframe)
		if err != nil {
			return err
		}

		archiveFilePathCSV := asset.SetRef.Settings.BuildArchiveFilePath(asset.Type(), date, "csv")
		archiveFilePathZIP := asset.SetRef.Settings.BuildArchiveFilePath(asset.Type(), date, "zip")

		archiveFolderPath := asset.SetRef.Settings.BuildArchiveFolderPath(asset.Type())

		defer func() {
			if os.Remove(archiveFilePathCSV) == nil {
				runner.SetStatValue("CSV_FILE_REMOVED", 1)
			}
		}()

		archiveZipSize, err := pcommon.File.GetFileSize(archiveFilePathZIP)
		if err != nil {
			return err
		}

		runner.SetStatValue(STAT_VALUE_ARCHIVE_SIZE, archiveZipSize)

		go func() {
			time.Sleep(2 * time.Second)
			for runner.IsRunning() {
				printStateParsingStatus(runner, asset)
				time.Sleep(5 * time.Second)
			}
		}()

		err = pcommon.File.UnzipFile(archiveFilePathZIP, archiveFolderPath)
		if err != nil {
			if err.Error() == "zip: not a valid zip file" {
				os.Remove(archiveFilePathZIP)
			}
			return err
		}

		runner.AddStep()
		csvLines, err := parseFromCSV(asset, date)
		if err != nil {
			return err
		}
		if len(csvLines) == 0 {
			log.WithFields(log.Fields{
				"set":   asset.SetRef.ID(),
				"asset": asset.Address(),
				"date":  date,
			}).Warn("No data found in CSV file")
		}

		runner.SetSize().Max(int64(len(csvLines)))
		runner.AddStep()
		dataList := aggregateLinesToValuesToPrices(csvLines, asset)
		for _, tick := range dataList.Map() {
			prevState.CheckUpdateMax(tick.Max(), tick.GetTime())
			prevState.CheckUpdateMin(tick.Min(), tick.GetTime())
		}

		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(dataList.Len()))
		runner.AddStep()
		if err := asset.Store(dataList.ToRaw(asset.Decimals()), timeframe, prevState.Copy(), pcommon.NewTimeUnitFromTime(dateTime).Add(time.Hour*24)); err != nil {
			return err
		}

		runner.AddStep()
		printStateParsingStatus(runner, asset)
		return nil
	}
	runner.AddProcess(process)
}

type CSVLine struct {
	Timestamp pcommon.TimeUnit
	Value     float64
}

func parseFromCSVLine(fields []string) (CSVLine, error) {
	var err error
	csv := CSVLine{}

	timestamp, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return CSVLine{}, err
	}
	csv.Timestamp = pcommon.NewTimeUnit(timestamp)

	csv.Value, err = strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return CSVLine{}, err
	}
	return csv, nil
}

func parseFromCSV(asset *setlib.AssetState, date string) ([]CSVLine, error) {
	file, err := os.Open(asset.SetRef.Settings.BuildArchiveFilePath(asset.Type(), date, "csv"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ',' // Set the delimiter to comma
	reader.TrimLeadingSpace = true

	var lines []CSVLine

	// Check if the CSV is empty
	firstRow, err := reader.Read()
	if err == io.EOF {
		// CSV is empty, return an empty slice
		return lines, nil
	}
	if err != nil {
		return nil, err
	}

	// Determine if the first row is a header or a data row
	if isHeader(firstRow) {
		// Read the next row if the first row is a header
		firstRow, err = reader.Read()
		if err == io.EOF {
			// CSV only contains a header, return an empty slice
			return lines, nil
		}
		if err != nil {
			return nil, err
		}
	}

	line, err := parseFromCSVLine(firstRow)
	if err != nil {
		return nil, err
	}
	lines = append(lines, line)

	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		line, err := parseFromCSVLine(fields)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)

		// Convert fields to a trade and append to trades
	}
	return lines, nil
}

// Example function to determine if a row is a header
func isHeader(row []string) bool {
	for _, field := range row {
		if strings.Contains(field, "time") || strings.Contains(field, "date") || strings.Contains(field, "id") {
			return true
		}
	}
	return false
}

func aggregateLinesToValuesToPrices(lines []CSVLine, state *setlib.AssetState) pcommon.DataList {
	bucket := pcommon.NewTypeTimeArray(state.DataType())
	tmpList := pcommon.NewTypeTimeArray(state.DataType())

	prevTime := pcommon.TimeUnit(0)
	for _, line := range lines {
		div := pcommon.TimeUnit(0).Add(pcommon.Env.MIN_TIME_FRAME)
		currentTime := line.Timestamp
		if div > 0 {
			currentTime /= div
			currentTime *= div
		}
		if prevTime == 0 {
			tmpList = tmpList.Append(pcommon.NewTypeTime(state.DataType(), line.Value, currentTime))
		} else {
			if prevTime == currentTime {
				tmpList = tmpList.Append(pcommon.NewTypeTime(state.DataType(), line.Value, currentTime))
			} else {
				bucket = bucket.Append(tmpList.Aggregate(pcommon.Env.MIN_TIME_FRAME, prevTime))
				tmpList = pcommon.NewTypeTimeArray(state.DataType()).Append(pcommon.NewTypeTime(state.DataType(), line.Value, currentTime))
			}
		}
		prevTime = currentTime
	}
	if tmpList != nil && tmpList.Len() > 0 {
		bucket = bucket.Append(tmpList.Aggregate(pcommon.Env.MIN_TIME_FRAME, prevTime))
	}

	return bucket
}

func buildStateParsingRunner(state *setlib.AssetState, date string) *gorunner.Runner {
	runner := gorunner.NewRunner(STATE_PARSING_KEY + "-" + string(state.Address()) + "-" + date)

	addTimeframe(runner, pcommon.Env.MIN_TIME_FRAME)
	addDate(runner, date)
	addAssetAddresses(runner, []pcommon.AssetAddress{state.Address()})

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

	addStateParsingRunnerProcess(runner, state)
	return runner
}
