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

type CSVBuildingOrderPacked struct {
	From      int64      //In unix seconds
	To        int64      //In unix seconds
	Timeframe int64      //In milliseconds
	Orders    [][]string // [setID, assetID, columns...]
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
		return order.Asset.SetRef.ID() + ":" + order.Asset.ID()
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
	AssetStateIDs  []string         `json:"asset_state_ids"`
}

func buildCSVStatus(runner *gorunner.Runner, parameters *CSVBuildingOrder) CSVStatus {
	buildID := parameters.ID()

	status := CSVStatus{
		BuildID: buildID,
	}

	status.Percent = runner.Percent()

	if !runner.HasStarted() {
		status.Status = "SCHEDULED"
	} else if runner.HasStarted() && !runner.IsDone() {
		if runner.CountSteps() == 0 {
			status.Status = "WRITTING"
		}
		if runner.CountSteps() == 1 {
			status.Percent = 99.99
			status.Status = "ZIPPING"
		}
	} else if runner.IsDone() {
		status.Percent = 99.99
		status.Status = "DONE"
	}

	if runner.HasStarted() {
		status.Size = runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)
	}

	status.From = parameters.From
	status.To = parameters.To

	status.TimeframeLabel, _ = pcommon.Format.TimeFrameToLabel(parameters.Timeframe)

	status.AssetStateIDs = lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) string {
		return order.Asset.SetAndAssetID()
	})

	return status
}

func printBuildCSVStatus(runner *gorunner.Runner, parameter *CSVBuildingOrder) {

	csvStatus := buildCSVStatus(runner, parameter)

	if runner.CountSteps() == 0 {
		log.WithFields(log.Fields{
			"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
			"buildID":  csvStatus.BuildID,
			"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
		}).Info(fmt.Sprintf("Building CSV archive for %s", ""))
	}

	if runner.CountSteps() == 1 {

		log.WithFields(log.Fields{
			"size":    pcommon.Format.LargeBytesToShortString(runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)),
			"buildID": csvStatus.BuildID,
		}).Info(fmt.Sprintf("Zipping CSV archive for %s", ""))
	}

	if runner.CountSteps() == 2 {

		log.WithFields(log.Fields{
			"size":    pcommon.Format.LargeBytesToShortString(runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)),
			"buildID": csvStatus.BuildID,
			"done":    "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
		}).Info(fmt.Sprintf("Successfully built CSV for %s", ""))
	}
}

func BuildCSVFolderPath() string {
	dir := os.Getenv("CSV_DIR")
	if dir == "" {
		log.Fatal("CSV_DIR is not set")
	}
	if err := pcommon.File.EnsureDir(dir); err != nil {
		log.Fatal("Error creating CSV_DIR folder")
	}
	return dir
}

func BuildCSVArchiveFolderPath(buildID string) string {
	p := filepath.Join(BuildCSVFolderPath(), buildID)
	if err := pcommon.File.EnsureDir(p); err != nil {
		log.Fatal("Error creating CSV archive folder")
	}
	return p
}

func buildCSV(runner *gorunner.Runner, parameters *CSVBuildingOrder) error {

	runner.SetSize().Initial(parameters.From.Int())
	runner.SetSize().Max(parameters.To.Int())

	headerDouble := lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) []string {
		return order.Asset.Type().Header(order.Asset.SetAndAssetID(), order.Columns)
	})
	header := []string{}
	for _, h := range headerDouble {
		header = append(header, h...)
	}

	wg := sync.WaitGroup{}
	orders := parameters.Orders

	froms := make([]pcommon.TimeUnit, len(orders))

	muFrom := sync.RWMutex{}
	getFrom := func(pos int) pcommon.TimeUnit {
		muFrom.RLock()
		defer muFrom.RUnlock()
		return froms[pos]
	}
	setNewFrom := func(pos int, newFrom pcommon.TimeUnit) {
		muFrom.Lock()
		defer muFrom.Unlock()
		froms[pos] = newFrom
	}

	var stopErr error = nil
	muStopErr := sync.RWMutex{}
	setStopErr := func(err error) {
		muStopErr.Lock()
		defer muStopErr.Unlock()
		stopErr = err
	}

	BATCH_LIMIT := 50_000
	if parameters.Timeframe > time.Minute {
		BATCH_LIMIT = 10_000
	} else if parameters.Timeframe > time.Second*15 {
		BATCH_LIMIT = 20_000
	}

	interval := time.Duration(BATCH_LIMIT) * parameters.Timeframe

	for i, order := range orders {
		minFrom := order.Asset.DataT0()
		froms[i] = pcommon.TimeUnit(math.Max(float64(minFrom), float64(parameters.From)))
	}

	turn := 0
	fileCount := 0
	cumulatedWrittenSize := 0
	var cumulatedMemorizedSize int64 = 0

	var file *os.File = nil
	var writer *csv.Writer = nil

	go func() {
		time.Sleep(time.Second * 2)
		for runner.Task.IsRunning() {
			printBuildCSVStatus(runner, parameters)
			time.Sleep(5 * time.Second)
		}
	}()

	for {
		wg.Add(len(orders))
		listData := map[string]dtype.DataList{}

		for i, order := range orders {
			go func(pos int, state *setlib.AssetState) {
				defer wg.Done()
				from := getFrom(pos)
				if from > parameters.To {
					return
				}

				to := from.Add(interval)
				list, err := state.GetInDataRange(from, to, parameters.Timeframe, nil, nil)
				if err != nil {
					setStopErr(err)
					return
				}
				setNewFrom(pos, to)
				lengt, _ := util.Len(list)
				if lengt == 0 {
					setNewFrom(pos, parameters.To+1)
					return
				}
				listData[state.SetAndAssetID()] = list
			}(i, order.Asset)
		}
		wg.Wait()
		if stopErr != nil {
			return stopErr
		}

		totalSingleData := 0
		for _, list := range listData {
			l, err := util.Len(list)
			if err != nil {
				return err
			}
			totalSingleData += l
		}
		if totalSingleData == 0 {
			break
		}

		lines := []string{}
		for {
			minTime := pcommon.NewTimeUnitFromTime(time.Now())
			minTimeState := ""

			for _, order := range orders {
				list := listData[order.Asset.SetAndAssetID()]
				if l, _ := util.Len(list); l == 0 {
					continue
				}

				if order.Asset.IsPoint() {
					cast := list.(dtype.PointTimeArray)
					p := cast[0]
					if p.Time < minTime {
						minTime = p.Time
						minTimeState = order.Asset.SetAndAssetID()
					}
				} else if order.Asset.IsUnit() {
					cast := list.(dtype.UnitTimeArray)
					u := cast[0]
					if u.Time < minTime {
						minTime = u.Time
						minTimeState = order.Asset.SetAndAssetID()
					}
				} else if order.Asset.IsQuantity() {
					cast := list.(dtype.QuantityTimeArray)
					q := cast[0]
					if q.Time < minTime {
						minTime = q.Time
						minTimeState = order.Asset.SetAndAssetID()
					}
				}
			}

			if minTime > parameters.To || minTimeState == "" {
				break
			}

			line := []string{}
			for _, order := range orders {
				precision := order.Asset.Precision()
				assetStateID := order.Asset.SetAndAssetID()

				list := listData[assetStateID]
				if l, _ := util.Len(list); l == 0 {
					continue
				}

				if order.Asset.IsPoint() {
					cast := list.(dtype.PointTimeArray)
					p := cast[0]
					if p.Time == minTime {
						line = append(line, p.CSVLine(assetStateID, precision, order.Columns)...)
						listData[assetStateID] = cast[1:]
					} else {
						line = append(line, dtype.PointTime{}.CSVLine(assetStateID, precision, order.Columns)...)
					}
				} else if order.Asset.IsUnit() {
					cast := list.(dtype.UnitTimeArray)
					u := cast[0]
					if u.Time == minTime {
						line = append(line, u.CSVLine(assetStateID, precision, order.Columns)...)
						listData[assetStateID] = cast[1:]
					} else {
						line = append(line, dtype.UnitTime{}.CSVLine(assetStateID, precision, order.Columns)...)
					}
				} else if order.Asset.IsQuantity() {
					cast := list.(dtype.QuantityTimeArray)
					q := cast[0]
					if q.Time == minTime {
						line = append(line, q.CSVLine(assetStateID, precision, order.Columns)...)
						listData[assetStateID] = cast[1:]
					} else {
						line = append(line, dtype.QuantityTime{}.CSVLine(assetStateID, precision, order.Columns)...)
					}
				}
			}
			lines = append(lines, strings.Join(line, ","))
		}

		if len(lines) == 0 {
			break
		}

		var err error = nil

		if cumulatedWrittenSize == 0 {
			csvFilePath := filepath.Join(BuildCSVArchiveFolderPath(parameters.ID()), fmt.Sprintf("%d.csv", fileCount))
			file, err = os.Create(csvFilePath)
			if err != nil {
				return err
			}
			writer = csv.NewWriter(file)
			if err := writer.Write(header); err != nil {
				return err
			}
			headerSize := len(strings.Join(header, ",")) + 1
			cumulatedWrittenSize += headerSize
			runner.IncrementStatValue(STAT_VALUE_ARCHIVE_SIZE, int64(headerSize))
		}

		linesSize := 0
		for _, line := range lines {
			linesSize += len(line) + 1
			if err := writer.Write(strings.Split(line, ",")); err != nil {
				return err
			}
		}
		cumulatedWrittenSize += linesSize
		// runner.SetSize().Current(newFrom.Int(), false)
		runner.IncrementStatValue(STAT_VALUE_ARCHIVE_SIZE, int64(linesSize))
		runner.IncrementStatValue(STAT_VALUE_LINE_COUNT, int64(len(lines)))

		if cumulatedMemorizedSize > MAX_SIZE_CSV_FILE_BYTES {
			writer.Flush()
		}

		if int64(cumulatedWrittenSize) > MAX_SIZE_CSV_FILE_BYTES {
			if err := file.Close(); err != nil {
				return err
			}
			writer.Flush()
			fileCount++
			cumulatedWrittenSize = 0
		}
		turn++
	}

	if file != nil {
		writer.Flush()
		file.Close()
	}

	runner.AddStep()
	folderPath := BuildCSVArchiveFolderPath(parameters.ID())
	if err := pcommon.File.ZipDirectory(folderPath, folderPath+".zip"); err != nil {
		return err
	}
	runner.AddStep()
	go os.RemoveAll(folderPath)
	printBuildCSVStatus(runner, parameters)
	return nil
}

func buildCSVBuildingRunner(parameters *CSVBuildingOrder) *gorunner.Runner {
	buildID := CSV_BUILDING_KEY + "-" + parameters.ID()
	runner := gorunner.NewRunner(buildID)

	fullIDs := lo.Map(parameters.Orders, func(order CSVAssetOrder, idx int) string {
		return order.Asset.SetAndAssetID()
	})
	fmt.Println(fullIDs)

	addAssetAndSetIDs(runner, fullIDs)
	addTimeframe(runner, parameters.Timeframe)

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
		return buildCSV(runner, parameters)
	})

	return runner
}
