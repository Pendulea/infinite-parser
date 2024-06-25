package engine

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"pendulev2/dtype"
	setlib "pendulev2/set2"
	"pendulev2/util"
	"sort"
	"strconv"
	"strings"
	"sync"
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

type CSVAssetOrder struct {
	Asset   *setlib.AssetState
	Columns dtype.CSVCheckListRequirement
}

type CSVBuildingOrder struct {
	From      pcommon.TimeUnit
	To        pcommon.TimeUnit
	Timeframe time.Duration
	Orders    []CSVAssetOrder
}

func (cbo *CSVBuildingOrder) ID() string {
	label, _ := pcommon.Format.TimeFrameToLabel(cbo.Timeframe)
	from := cbo.From.ToTime().Unix()
	to := cbo.To.ToTime().Unix()
	id := fmt.Sprintf("%s-%d-%d-", label, from, to)

	id2 := []string{}
	for _, order := range cbo.Orders {
		id2 = append(id2, order.Asset.SetRef.ID()+":"+order.Asset.ID()+":"+strings.Join(order.Columns.Columns(), ","))
	}

	return id + strings.Join(id2, "|")
}

func CSVBuildingOrderIDToStatus(id string, file pcommon.FileInfo) CSVStatus {
	idSplit := strings.Split(id, "-")
	from, _ := strconv.ParseInt(idSplit[1], 10, 64)
	to, _ := strconv.ParseInt(idSplit[2], 10, 64)

	status := CSVStatus{
		BuildID:        id,
		From:           pcommon.NewTimeUnit(from),
		To:             pcommon.NewTimeUnit(to),
		TimeframeLabel: idSplit[0],
		RequestTime:    file.Time,
		Status:         "DONE",
		Percent:        100,
		Size:           file.Size,
	}

	listAssets := strings.Split(idSplit[3], "|")
	for _, asset := range listAssets {
		assetSplit := strings.Split(asset, ":")
		setID := assetSplit[0]
		assetID := assetSplit[1]
		columns := strings.Split(assetSplit[2], ",")
		line := []string{setID, assetID}
		line = append(line, columns...)
		status.Assets = append(status.Assets, line)
	}
	return status
}

type CSVBuildingOrderPacked struct {
	From      int64      `json:"from"`      //In unix seconds `json:"from"`
	To        int64      `json:"to"`        //In unix seconds
	Timeframe int64      `json:"timeframe"` //In milliseconds
	Orders    [][]string `json:"orders"`    // [setID, assetID, columns...]
}

func parsePackedOrder(sets setlib.WorkingSets, order CSVBuildingOrderPacked) (*CSVBuildingOrder, error) {
	from := pcommon.NewTimeUnitFromTime(time.Unix(order.From, 0))
	to := pcommon.NewTimeUnitFromTime(time.Unix(order.To, 0))

	if to <= from {
		return nil, fmt.Errorf("to must be greater than from")
	}

	timeframe := time.Millisecond * time.Duration(order.Timeframe)
	// Check if timeframe is valid
	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return nil, err
	}

	orders := []CSVAssetOrder{}
	usedSets := make(map[string]*setlib.Set)

	for _, orderPacked := range order.Orders {
		if len := len(orderPacked); len < 3 {
			return nil, fmt.Errorf("order has invalid length %d", len)
		}
		setID := orderPacked[0]
		assetID := orderPacked[1]
		columns := orderPacked[2:]

		if usedSets[setID] == nil {
			set := sets.Find(setID)
			if set == nil {
				return nil, fmt.Errorf("set %s not found", setID)
			}
			usedSets[setID] = set
		}

		asset := usedSets[setID].Assets[assetID]
		if !asset.IsTimeframeSupported(timeframe) {
			return nil, fmt.Errorf("asset %s does not support timeframe %s", assetID, timeframe)
		}
		lct, err := asset.GetLastConsistencyTime(timeframe)
		if err != nil {
			return nil, err
		}
		if lct < to {
			return nil, fmt.Errorf("asset %s is not consistent until %s", assetID, lct)
		}
		requirements := dtype.CSVCheckListRequirement{}
		for _, column := range columns {
			if lo.IndexOf[string](asset.Type().Columns(), column) == -1 {
				return nil, fmt.Errorf("asset %s does not have column %s", assetID, column)
			}
			requirements[column] = true
		}

		orders = append(orders, CSVAssetOrder{asset, requirements})
	}

	orders = lo.UniqBy(orders, func(order CSVAssetOrder) string {
		return order.Asset.SetAndAssetID()
	})

	//order asc by theirs asset ID and state id
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].Asset.SetRef.ID() < orders[j].Asset.SetRef.ID() || (orders[i].Asset.SetRef.ID() == orders[j].Asset.SetRef.ID() && orders[i].Asset.ID() < orders[j].Asset.ID())
	})

	return &CSVBuildingOrder{from, to, timeframe, orders}, nil
}

type CSVStatus struct {
	BuildID        string           `json:"build_id"`
	RequestTime    int64            `json:"request_time"`
	Status         string           `json:"status"`
	Size           int64            `json:"size"`
	Percent        float64          `json:"percent"`
	From           pcommon.TimeUnit `json:"from"`
	To             pcommon.TimeUnit `json:"to"`
	TimeframeLabel string           `json:"timeframe_label"`
	Assets         [][]string       `json:"assets"`
}

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

func (parameters *CSVBuildingOrder) Status(runner *gorunner.Runner) CSVStatus {
	buildID := parameters.ID()

	status := CSVStatus{
		BuildID: buildID,
	}

	status.Percent = runner.Percent()
	status.Status = runnerStatusString(runner)
	status.Size = runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)
	status.From = parameters.From
	status.To = parameters.To
	status.TimeframeLabel, _ = pcommon.Format.TimeFrameToLabel(parameters.Timeframe)
	status.Assets = lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) []string {
		list := []string{order.Asset.SetRef.ID(), order.Asset.ID()}
		list = append(list, order.Columns.Columns()...)
		return list
	})

	return status
}

func printBuildCSVStatus(runner *gorunner.Runner) {
	parameters := getParameters(runner)

	csvStatus := parameters.Status(runner)

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

func (parameters *CSVBuildingOrder) BuildCSVArchiveFolderPath() string {
	dir := os.Getenv("CSV_DIR")
	if dir == "" {
		log.Fatal("CSV_DIR is not set")
	}
	if err := pcommon.File.EnsureDir(dir); err != nil {
		log.Fatal("Error creating CSV_DIR folder")
	}

	id := parameters.ID()
	p := filepath.Join(dir, id)
	if err := pcommon.File.EnsureDir(p); err != nil {
		log.Fatal("Error creating CSV archive folder")
	}
	return p
}

func (parameters *CSVBuildingOrder) buildQuerySummaryFile(runner *gorunner.Runner) error {
	status := parameters.Status(runner)

	lineWritten := runner.StatValue(STAT_VALUE_LINE_COUNT)

	setIDs := lo.Uniq(lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) string {
		return order.Asset.SetRef.ID()
	}))

	content := fmt.Sprintf("Timeframe: %s\nBetween: %s 00:00:00 and %s\n\n", status.TimeframeLabel,
		parameters.From.ToTime().UTC().Format("2006-01-02"),
		parameters.To.ToTime().UTC().Format("2006-01-02 15:04:05"))

	for _, setID := range setIDs {
		listFullColumns := []string{}
		for _, order := range parameters.Orders {
			if order.Asset.SetRef.ID() == setID {
				listFullColumns = append(listFullColumns, "- "+order.Asset.ID()+" : "+strings.Join(order.Columns.Columns(), ", "))
			}
		}
		content += fmt.Sprintf("%s:\n%s\n", setID, strings.Join(listFullColumns, "\n"))
	}
	content += fmt.Sprintf("\n\nTotal lines: %s\n", pcommon.Format.LargeNumberToShortString(lineWritten))

	queryFile := filepath.Join(parameters.BuildCSVArchiveFolderPath(), "query.txt")
	return util.WriteToFile(queryFile, content)
}

func buildCSV(runner *gorunner.Runner) error {
	parameters := getParameters(runner)
	runner.SetSize().Initial(parameters.From.Int())
	runner.SetSize().Max(parameters.To.Int())

	header := parameters.buildCSVHeader()
	froms := parameters.buildOrderFromTimes()

	go parameters.monitorProgress(runner)

	file, writer, err := createCSVFile(parameters.BuildCSVArchiveFolderPath(), 0, header)
	if err != nil {
		return err
	}
	defer closeCSVFile(writer, file)

	for turn, fileCount, cumulatedWrittenSize := 0, 0, int64(0); ; turn++ {
		listData, stopErr := parameters.fetchOrderData(&froms)
		if stopErr != nil {
			return stopErr
		}

		lines := parameters.collectLines(&listData)
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
	if err := parameters.buildQuerySummaryFile(runner); err != nil {
		return err
	}
	if err := parameters.zipCSVArchive(); err != nil {
		return err
	}
	runner.AddStep()
	go os.RemoveAll(parameters.BuildCSVArchiveFolderPath())
	printBuildCSVStatus(runner)
	return nil
}

func (parameters *CSVBuildingOrder) zipCSVArchive() error {
	folderPath := parameters.BuildCSVArchiveFolderPath()
	return pcommon.File.ZipDirectory(folderPath, folderPath+".zip")
}

func (parameters *CSVBuildingOrder) buildCSVHeader() []string {
	headerDouble := lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) []string {
		return order.Asset.Type().Header(order.Asset.SetAndAssetID(), order.Columns)
	})
	var header []string
	for _, h := range headerDouble {
		header = append(header, h...)
	}
	return header
}

func (parameters *CSVBuildingOrder) buildOrderFromTimes() []pcommon.TimeUnit {
	froms := make([]pcommon.TimeUnit, len(parameters.Orders))
	for i, order := range parameters.Orders {
		minFrom := order.Asset.DataT0()
		froms[i] = pcommon.TimeUnit(math.Max(float64(minFrom), float64(parameters.From)))
	}
	return froms
}

func (parameters *CSVBuildingOrder) monitorProgress(runner *gorunner.Runner) {
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

func (parameters *CSVBuildingOrder) fetchOrderData(froms *[]pcommon.TimeUnit) (map[string]dtype.DataList, error) {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	listData := make(map[string]dtype.DataList)

	var stopErr error
	muStopErr := sync.RWMutex{}
	setStopErr := func(err error) {
		muStopErr.Lock()
		defer muStopErr.Unlock()
		stopErr = err
	}

	getFromTime := func(pos int) pcommon.TimeUnit {
		muFrom := sync.RWMutex{}
		muFrom.RLock()
		defer muFrom.RUnlock()
		return (*froms)[pos]
	}

	setNewFromTime := func(pos int, newFrom pcommon.TimeUnit) {
		muFrom := sync.RWMutex{}
		muFrom.Lock()
		defer muFrom.Unlock()
		(*froms)[pos] = newFrom
	}

	BATCH_LIMIT := 50_000
	if parameters.Timeframe > time.Minute {
		BATCH_LIMIT = 10_000
	} else if parameters.Timeframe > time.Second*15 {
		BATCH_LIMIT = 20_000
	}

	interval := time.Duration(BATCH_LIMIT) * parameters.Timeframe

	for i, order := range parameters.Orders {
		wg.Add(1)
		go func(pos int, state *setlib.AssetState) {
			defer wg.Done()
			from := getFromTime(pos)
			if from > parameters.To {
				return
			}

			to := from.Add(interval)
			data, err := state.GetInDataRange(from, to, parameters.Timeframe, nil, nil)
			if err != nil {
				setStopErr(err)
				return
			}

			setNewFromTime(pos, to)
			if len := data.Len(); len == 0 {
				setNewFromTime(pos, to+1)
				return
			}

			mu.Lock()
			listData[state.SetAndAssetID()] = data
			mu.Unlock()
		}(i, order.Asset)
	}
	wg.Wait()
	if stopErr != nil {
		return nil, stopErr
	}

	return listData, nil
}

func (parameters *CSVBuildingOrder) collectLines(listData *map[string]dtype.DataList) []string {
	var lines []string

	for {
		minTime, minTimeState := parameters.findMinTimeState(listData)
		if minTime > parameters.To || minTimeState == "" {
			break
		}

		line := parameters.createCSVLine(listData, minTime)
		lines = append(lines, strings.Join(line, ","))
	}

	return lines
}

func (parameters *CSVBuildingOrder) findMinTimeState(listData *map[string]dtype.DataList) (pcommon.TimeUnit, string) {
	minTime := pcommon.NewTimeUnitFromTime(time.Now())
	var minTimeState string

	for _, order := range parameters.Orders {
		list := (*listData)[order.Asset.SetAndAssetID()]
		if list == nil || list.Len() == 0 {
			continue
		}
		first := list.First()
		if first.GetTime() < minTime {
			minTime, minTimeState = first.GetTime(), order.Asset.SetAndAssetID()
		}
	}
	return minTime, minTimeState
}

func (parameters *CSVBuildingOrder) createCSVLine(listData *map[string]dtype.DataList, minTime pcommon.TimeUnit) []string {
	var line []string

	for _, order := range parameters.Orders {
		assetStateID := order.Asset.SetAndAssetID()
		list := (*listData)[assetStateID]
		if list == nil || list.Len() == 0 {
			continue
		}
		first := list.First()
		precision := order.Asset.Precision()
		columns := order.Columns

		var assetLine []string
		if first.GetTime() == minTime {
			assetLine = list.First().CSVLine(order.Asset.SetAndAssetID(), precision, columns)
		} else {
			assetLine = dtype.NewTypeTime(order.Asset.Type(), 0, 0).CSVLine(order.Asset.SetAndAssetID(), precision, columns)
		}

		line = append(line, assetLine...)
		(*listData)[assetStateID] = (*listData)[assetStateID].RemoveFirstN(1)
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

func getParameters(r *gorunner.Runner) *CSVBuildingOrder {
	p, ok := gorunner.GetArg[*CSVBuildingOrder](r.Args, ARG_VALUE_PARAMETERS)
	if !ok {
		log.Fatal("Parameters not found in runner")
	}
	return p
}

func buildCSVBuildingRunner(parameters *CSVBuildingOrder) *gorunner.Runner {
	buildID := CSV_BUILDING_KEY + "-" + parameters.ID()
	runner := gorunner.NewRunner(buildID)

	fullIDs := lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) string {
		return order.Asset.SetAndAssetID()
	})

	addAssetAndSetIDs(runner, fullIDs)
	addTimeframe(runner, parameters.Timeframe)
	runner.Args[ARG_VALUE_PARAMETERS] = parameters

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

	runner.AddProcess(func() error {
		return buildCSV(runner)
	})

	return runner
}
