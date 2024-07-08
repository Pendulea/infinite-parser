package set2

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"pendulev2/util"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
)

type CSVOrderHeader struct {
	Timeframe time.Duration    `json:"timeframe"`
	From      pcommon.TimeUnit `json:"from"`
	To        pcommon.TimeUnit `json:"to"`
}

type CSVAsset struct {
	Asset   *AssetState
	Columns pcommon.CSVCheckListRequirement
}

type CSVAssetList []CSVAsset

type CSVOrderPacked struct {
	Header CSVOrderHeader `json:"header"`
	Orders [][]string     `json:"orders"`
}

type CSVOrderUnpacked struct {
	Header CSVOrderHeader `json:"header"`
	Orders CSVAssetList   `json:"orders"`
}

func (c *CSVAssetList) BuildID(header CSVOrderHeader) string {
	label, _ := pcommon.Format.TimeFrameToLabel(header.Timeframe)
	hash := hex.EncodeToString(c.Sha256())
	id := fmt.Sprintf("%s-%d-%d-%s", label, header.From.ToTime().Unix(), header.To.ToTime().Unix(), hash)
	return id
}

func CSVIDToStatus(id string, file pcommon.FileInfo) pcommon.CSVStatus {
	idSplit := strings.Split(id, "-")
	from, _ := strconv.ParseInt(idSplit[1], 10, 64)
	to, _ := strconv.ParseInt(idSplit[2], 10, 64)

	status := pcommon.CSVStatus{
		BuildID:        id,
		From:           pcommon.NewTimeUnit(from),
		To:             pcommon.NewTimeUnit(to),
		TimeframeLabel: idSplit[0],
		RequestTime:    file.Time,
		Status:         "DONE",
		Percent:        100,
		Size:           file.Size,
	}

	return status
}

func ParseOrderHeaderFromID(id string) (*CSVOrderHeader, []byte, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 4 {
		return nil, nil, fmt.Errorf("invalid id %s", id)
	}
	label := parts[0]
	timeframe, err := pcommon.LabelToTimeFrame(label)
	if err != nil {
		return nil, nil, err
	}
	fromTime, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, nil, err
	}
	toTime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, nil, err
	}

	hash, err := hex.DecodeString(parts[3])
	if err != nil {
		return nil, nil, err
	}

	return &CSVOrderHeader{
		Timeframe: timeframe,
		From:      pcommon.NewTimeUnit(fromTime),
		To:        pcommon.NewTimeUnit(toTime),
	}, hash, nil
}

func (c *CSVAssetList) Sha256() []byte {
	cpy := make(CSVAssetList, len(*c))
	copy(cpy, *c)
	sort.Slice(cpy, func(i, j int) bool {
		// Access the elements using the indices
		a, b := cpy[i], cpy[j]
		colA := strings.Join(util.ColumnNamesToStrings(a.Columns.Columns()), ",")
		idA := string(a.Asset.Address()) + colA
		colB := strings.Join(util.ColumnNamesToStrings(b.Columns.Columns()), ",")
		idB := string(b.Asset.Address()) + colB
		return idA < idB
	})

	h := sha256.New()
	for _, order := range cpy {
		h.Write([]byte(order.Asset.Address()))
		h.Write([]byte(strings.Join(util.ColumnNamesToStrings(order.Columns.Columns()), ",")))
	}
	return h.Sum(nil)
}

func (order *CSVOrderUnpacked) BuildCSVArchiveFolderPath() string {
	dir := os.Getenv("CSV_DIR")
	if dir == "" {
		log.Fatal("CSV_DIR is not set")
	}
	if err := pcommon.File.EnsureDir(dir); err != nil {
		log.Fatal("Error creating CSV_DIR folder")
	}

	id := order.Orders.BuildID(order.Header)
	p := filepath.Join(dir, id)
	if err := pcommon.File.EnsureDir(p); err != nil {
		log.Fatal("Error creating CSV archive folder")
	}
	return p
}

func (order CSVOrderPacked) Unpack(sets WorkingSets) (*CSVOrderUnpacked, error) {
	from := order.Header.From
	to := order.Header.To

	if to <= from {
		return nil, fmt.Errorf("to must be greater than from")
	}

	timeframe := order.Header.Timeframe
	// Check if timeframe is valid
	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return nil, err
	}

	orders, err := parseArrayOrder(sets, timeframe, order.Orders)
	if err != nil {
		return nil, err
	}

	for _, order := range orders {
		lct, err := order.Asset.GetLastConsistencyTime(timeframe)
		if err != nil {
			return nil, err
		}
		if lct < to {
			return nil, fmt.Errorf("asset %s is not consistent until %s", order.Asset.Address(), lct)
		}
	}

	return &CSVOrderUnpacked{order.Header, orders}, nil
}

func parseArrayOrder(sets WorkingSets, timeframe time.Duration, listRawOrders [][]string) (CSVAssetList, error) {
	orders := CSVAssetList{}

	for _, orderPacked := range listRawOrders {

		if len := len(orderPacked); len < 2 {
			return nil, fmt.Errorf("order has invalid length %d", len)
		}
		assetAddress := pcommon.AssetAddress(orderPacked[0])
		assetAddressParsed, err := assetAddress.Parse()
		if err != nil {
			return nil, fmt.Errorf("asset address %s is invalid", assetAddress)
		}
		columns := orderPacked[1:]
		setID := assetAddressParsed.IDString()
		set := sets.Find(setID)
		if set == nil {
			return nil, fmt.Errorf("set %s not found", setID)
		}
		assetState := set.Assets[assetAddress]
		if assetState == nil {
			return nil, fmt.Errorf("asset %s not found in set %s", assetAddress, setID)
		}
		if !assetState.IsTimeframeSupported(timeframe) {
			return nil, fmt.Errorf("asset %s does not support timeframe %s", assetAddress, timeframe)
		}

		requirements := pcommon.CSVCheckListRequirement{}
		for _, column := range columns {
			if lo.IndexOf(assetState.DataType().Columns(), pcommon.ColumnName(column)) == -1 {
				return nil, fmt.Errorf("asset %s does not have column %s", assetAddress, column)
			}
			requirements[pcommon.ColumnName(column)] = true
		}

		orders = append(orders, CSVAsset{Asset: assetState, Columns: requirements})
	}

	orders = lo.UniqBy(orders, func(order CSVAsset) string {
		return string(order.Asset.Address())
	})

	sort.Slice(orders, func(i, j int) bool {
		return orders[i].Asset.Address() < orders[j].Asset.Address()
	})

	return orders, nil
}

func (parameters *CSVOrderUnpacked) ZipCSVArchive() error {
	folderPath := parameters.BuildCSVArchiveFolderPath()
	return pcommon.File.ZipDirectory(folderPath, folderPath+".zip")
}

func (parameters *CSVOrderUnpacked) BuildCSVHeader() ([]string, error) {
	countSetIDs := len(lo.UniqBy(parameters.Orders, func(order CSVAsset) string {
		return order.Asset.SetRef.ID()
	}))

	var eRR error = nil
	headerDouble := lo.Map(parameters.Orders, func(order CSVAsset, idx int) []string {
		prefix, err := order.Asset.ParsedAddress().BuildCSVColumnName(countSetIDs > 1)
		if err != nil {
			eRR = err
			return nil
		}
		return order.Asset.DataType().Header(prefix, order.Columns)
	})
	if eRR != nil {
		return nil, eRR
	}
	var header []string
	for _, h := range headerDouble {
		header = append(header, h...)
	}
	return header, nil
}

func (parameters *CSVOrderUnpacked) BuildOrderFromTimes() []pcommon.TimeUnit {
	froms := make([]pcommon.TimeUnit, len(parameters.Orders))
	for i, order := range parameters.Orders {
		minFrom := order.Asset.DataHistoryTime0()
		froms[i] = pcommon.TimeUnit(math.Max(float64(minFrom), float64(parameters.Header.From)))
	}
	return froms
}

func (parameters *CSVOrderUnpacked) FetchOrderData(froms *[]pcommon.TimeUnit) (map[pcommon.AssetAddress]pcommon.DataList, error) {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	listData := make(map[pcommon.AssetAddress]pcommon.DataList)

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
	if parameters.Header.Timeframe > time.Minute {
		BATCH_LIMIT = 10_000
	} else if parameters.Header.Timeframe > time.Second*15 {
		BATCH_LIMIT = 20_000
	}

	interval := time.Duration(BATCH_LIMIT) * parameters.Header.Timeframe

	for i, order := range parameters.Orders {
		wg.Add(1)
		go func(pos int, state *AssetState) {
			defer wg.Done()
			from := getFromTime(pos)
			if from > parameters.Header.To {
				return
			}

			to := from.Add(interval)
			data, err := state.GetInDataRange(from, to, parameters.Header.Timeframe, nil, nil)
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
			listData[state.Address()] = data
			mu.Unlock()
		}(i, order.Asset)
	}
	wg.Wait()
	if stopErr != nil {
		return nil, stopErr
	}

	return listData, nil
}
