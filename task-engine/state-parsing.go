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

	if runner.IsRunning() {
		if runner.CountSteps() == 0 {
			archiveSize := runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)

			log.WithFields(log.Fields{
				"size": pcommon.Format.LargeBytesToShortString(archiveSize),
			}).Info(fmt.Sprintf("Unzipping %s %s archive (%s)", asset.SetRef.ID(), asset.ID(), date))
			return
		} else if runner.CountSteps() == 1 {
			archiveSize := runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)

			log.WithFields(log.Fields{
				"size": pcommon.Format.LargeBytesToShortString(int64(float64(archiveSize) * 5.133)),
			}).Info(fmt.Sprintf("Parsing %s %s (%s)", asset.SetRef.ID(), asset.ID(), date))

		} else if runner.CountSteps() == 2 {
			tradeParsed := pcommon.Format.LargeNumberToShortString(runner.Size().Current()) + "/" + pcommon.Format.LargeNumberToShortString(runner.Size().Max())

			log.WithFields(log.Fields{
				"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
				"speed":    pcommon.Format.LargeNumberToShortString(int64(runner.SizePerMillisecond()*1000)) + " line/s",
				"total":    tradeParsed,
				"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
			}).Info(fmt.Sprintf("Building %s %s (%s)", asset.SetRef.ID(), asset.ID(), date))
		} else if runner.CountSteps() == 3 {
			totalRows := runner.StatValue(STAT_VALUE_DATA_COUNT)
			log.WithFields(log.Fields{
				"aggregated": pcommon.Format.LargeNumberToShortString(totalRows),
				"parsed":     pcommon.Format.LargeNumberToShortString(runner.Size().Max()),
			}).Info(fmt.Sprintf("Storing %s %s (%s)", asset.SetRef.ID(), asset.ID(), date))

		} else if runner.CountSteps() >= 4 {
			totalRows := runner.StatValue(STAT_VALUE_DATA_COUNT)

			log.WithFields(log.Fields{
				"aggregated": pcommon.Format.LargeNumberToShortString(totalRows),
				"parsed":     pcommon.Format.LargeNumberToShortString(runner.Size().Max()),
				"done":       "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
			}).Info(fmt.Sprintf("Successfully stored %s %s (%s)", asset.SetRef.ID(), asset.ID(), date))
		}
	}
}

func addStateParsingRunnerProcess(runner *gorunner.Runner, state *setlib.AssetState) {
	process := func() error {
		date := getDate(runner)

		dateTime, err := pcommon.Format.StrDateToDate(date)
		if err != nil {
			return err
		}

		archiveFilePathCSV := state.SetRef.Settings.BuildArchiveFilePath(state.ID(), date, "csv")
		archiveFilePathZIP := state.SetRef.Settings.BuildArchiveFilePath(state.ID(), date, "zip")

		archiveFolderPath := state.SetRef.Settings.BuildArchiveFolderPath(state.ID())

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
				printStateParsingStatus(runner, state)
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
		csvLines, err := ParseFromCSV(state, date)
		if err != nil {
			return err
		}
		if len(csvLines) == 0 {
			log.WithFields(log.Fields{
				"set":   state.SetRef.ID(),
				"asset": state.ID(),
				"date":  date,
			}).Warn("No data found in CSV file")
		}

		runner.SetSize().Max(int64(len(csvLines)))
		runner.AddStep()
		dataList := AggregateLinesToValuesToPrices(csvLines, state)
		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(dataList.Len()))
		runner.AddStep()

		if err := state.Store(dataList.ToRaw(state.Precision()), pcommon.Env.MIN_TIME_FRAME, pcommon.NewTimeUnitFromTime(dateTime).Add(time.Hour*24)); err != nil {
			return err
		}

		runner.AddStep()
		printStateParsingStatus(runner, state)
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

func ParseFromCSV(asset *setlib.AssetState, date string) ([]CSVLine, error) {
	file, err := os.Open(asset.SetRef.Settings.BuildArchiveFilePath(asset.ID(), date, "csv"))
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

func AggregateLinesToValuesToPrices(lines []CSVLine, state *setlib.AssetState) pcommon.DataList {
	bucket := pcommon.NewTypeTimeArray(state.Type())
	tmpList := pcommon.NewTypeTimeArray(state.Type())

	prevTime := pcommon.TimeUnit(0)
	for _, line := range lines {
		div := pcommon.TimeUnit(0).Add(pcommon.Env.MIN_TIME_FRAME)
		currentTime := line.Timestamp
		if div > 0 {
			currentTime /= div
			currentTime *= div
		}
		if prevTime == 0 {
			tmpList = tmpList.Append(pcommon.NewTypeTime(state.Type(), line.Value, currentTime))
		} else {
			if prevTime == currentTime {
				tmpList = tmpList.Append(pcommon.NewTypeTime(state.Type(), line.Value, currentTime))
			} else {
				bucket = bucket.Append(tmpList.Aggregate(pcommon.Env.MIN_TIME_FRAME, prevTime))
				tmpList = pcommon.NewTypeTimeArray(state.Type()).Append(pcommon.NewTypeTime(state.Type(), line.Value, currentTime))
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
	runner := gorunner.NewRunner(STATE_PARSING_KEY + "-" + state.SetAndAssetID() + "-" + date)

	addTimeframe(runner, pcommon.Env.MIN_TIME_FRAME)
	addDate(runner, date)
	addAssetAndSetIDs(runner, []string{state.SetAndAssetID()})

	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
		for _, r := range details.RunningRunners {
			if !haveSameFullIDs(r, runner) {
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
