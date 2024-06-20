package engine

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	setlib "pendulev2/set2"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	TRADE_PARSING_KEY = "trade_parsing"

	STAT_VALUE_ARCHIVE_SIZE = "ARCHIVE_SIZE"
	STAT_VALUE_DATA_COUNT   = "DATA_COUNT"
)

func printTradeParsingStatus(runner *gorunner.Runner, setID string) {
	date := getDate(runner)

	assetStateIDs := parseAssetStateIDs(runner)

	if runner.IsRunning() {
		if runner.CountSteps() == 0 {
			archiveSize := runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)

			log.WithFields(log.Fields{
				"size": pcommon.Format.LargeBytesToShortString(archiveSize),
			}).Info(fmt.Sprintf("Unzipping %s trade archive (%s)", setID, date))
			return
		} else if runner.CountSteps() == 1 {
			archiveSize := runner.StatValue(STAT_VALUE_ARCHIVE_SIZE)

			log.WithFields(log.Fields{
				"size": pcommon.Format.LargeBytesToShortString(int64(float64(archiveSize) * 5.133)),
			}).Info(fmt.Sprintf("Parsing %s trades (%s)", setID, date))

		} else if runner.CountSteps() == 2 {
			tradeParsed := pcommon.Format.LargeNumberToShortString(runner.Size().Current()) + "/" + pcommon.Format.LargeNumberToShortString(runner.Size().Max())

			log.WithFields(log.Fields{
				"progress": fmt.Sprintf("%.2f%%", runner.Percent()),
				"speed":    pcommon.Format.LargeNumberToShortString(int64(runner.SizePerMillisecond()*1000)) + " trades/s",
				"total":    tradeParsed,
				"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
			}).Info(fmt.Sprintf("Building %s %s columns (%s)", setID, strings.Join(assetStateIDs, ", "), date))
		} else if runner.CountSteps() == 3 {
			totalRows := runner.StatValue(STAT_VALUE_DATA_COUNT)
			log.WithFields(log.Fields{
				"rows":  pcommon.Format.LargeNumberToShortString(totalRows),
				"trade": pcommon.Format.LargeNumberToShortString(runner.Size().Max()),
			}).Info(fmt.Sprintf("Storing %s %s columns... (%s)", setID, strings.Join(assetStateIDs, ", "), date))

		} else if runner.CountSteps() >= 4 {
			totalRows := runner.StatValue(STAT_VALUE_DATA_COUNT)

			log.WithFields(log.Fields{
				"rows":  pcommon.Format.LargeNumberToShortString(totalRows),
				"trade": pcommon.Format.LargeNumberToShortString(runner.Size().Max()),
				"done":  "+" + pcommon.Format.AccurateHumanize(runner.Timer()),
			}).Info(fmt.Sprintf("Successfully stored %s %s columns (%s)", setID, strings.Join(assetStateIDs, ", "), date))
		}
	}
}

func addTradeParsingRunnerProcess(runner *gorunner.Runner, pair *pcommon.Pair) {
	process := func() error {
		date := getDate(runner)

		set := Engine.Sets.Find(pair.BuildSetID())
		if set == nil {
			return fmt.Errorf("set not found")
		}

		archiveTradesFilePathCSV := pair.BuildTradesArchivesFilePath(date, "csv")
		archiveTradesFilePathZIP := pair.BuildTradesArchivesFilePath(date, "zip")
		archiveTradesFolderPath := pair.BuildTradesArchiveFolderPath()

		defer func() {
			if os.Remove(archiveTradesFilePathCSV) == nil {
				runner.SetStatValue("CSV_FILE_REMOVED", 1)
			}
		}()

		archiveZipSize, err := pcommon.File.GetFileSize(archiveTradesFilePathZIP)
		if err != nil {
			return err
		}

		runner.SetStatValue(STAT_VALUE_ARCHIVE_SIZE, archiveZipSize)

		go func() {
			time.Sleep(2 * time.Second)
			for runner.IsRunning() {
				printTradeParsingStatus(runner, set.ID())
				time.Sleep(5 * time.Second)
			}
		}()

		err = pcommon.File.UnzipFile(archiveTradesFilePathZIP, archiveTradesFolderPath)
		if err != nil {
			if err.Error() == "zip: not a valid zip file" {
				os.Remove(archiveTradesFilePathZIP)
			}
			return err
		}

		runner.AddStep()

		trades, err := pair.ParseTradesFromCSV(date)
		if err != nil {
			return err
		}
		if len(trades) == 0 {
			return fmt.Errorf("no trades found")
		}

		runner.SetSize().Max(int64(len(trades)))
		runner.AddStep()

		var prices setlib.UnitTimeArray
		var volumes setlib.QuantityTimeArray

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			prices = AggregateTradesToPrices(trades)
			wg.Done()
		}()
		go func() {
			volumes = AggregateTradesToVolumes(trades)
			wg.Done()
		}()
		wg.Wait()

		runner.SetStatValue(STAT_VALUE_DATA_COUNT, int64(len(prices)))
		runner.AddStep()
		wg.Add(2)

		var err0, err1 error
		go func() {
			state := set.Assets[setlib.PRICE]
			err0 = state.Store(prices.ToRaw(state.Precision()), pcommon.Env.MIN_TIME_FRAME, -1)
			wg.Done()
		}()
		go func() {
			state := set.Assets[setlib.VOLUME]
			err1 = state.Store(volumes.ToRaw(state.Precision()), pcommon.Env.MIN_TIME_FRAME, -1)
			wg.Done()
		}()

		wg.Wait()
		if err0 != nil {
			return err0
		}
		if err1 != nil {
			return err1
		}

		if err := updateAllConsistencyTime(set, parseAssetStateIDs(runner), date); err != nil {
			return err
		}

		runner.AddStep()
		printTradeParsingStatus(runner, set.ID())
		return nil
	}
	runner.AddProcess(process)
}

func AggregateTradesToPrices(trades pcommon.TradeList) setlib.UnitTimeArray {
	pricesBucket := setlib.UnitTimeArray{}
	aggrUnitList := setlib.UnitTimeArray{}

	for _, trade := range trades {
		div := pcommon.TimeUnit(0).Add(pcommon.Env.MIN_TIME_FRAME)
		currentTime := trade.Timestamp
		if div > 0 {
			currentTime /= div
			currentTime *= div
		}

		aggSize := len(aggrUnitList)
		if aggSize == 0 {
			aggrUnitList = append(aggrUnitList, setlib.NewUnit(trade.Price).ToTime(currentTime))
		} else {
			prev := aggrUnitList[aggSize-1]
			if prev.Time == currentTime {
				aggrUnitList = append(aggrUnitList, setlib.NewUnit(trade.Price).ToTime(currentTime))
			} else {
				pricesBucket = append(pricesBucket, aggrUnitList.Aggregate(pcommon.Env.MIN_TIME_FRAME, currentTime).(setlib.UnitTime))
				aggrUnitList = setlib.UnitTimeArray{}
			}
		}
	}
	if len(aggrUnitList) > 0 {
		pricesBucket = append(pricesBucket, aggrUnitList.Aggregate(pcommon.Env.MIN_TIME_FRAME, aggrUnitList[len(aggrUnitList)-1].Time).(setlib.UnitTime))
	}
	return pricesBucket
}

func AggregateTradesToVolumes(trades pcommon.TradeList) setlib.QuantityTimeArray {
	volumesBucket := setlib.QuantityTimeArray{}
	aggrQtyList := setlib.QuantityTimeArray{}

	for _, trade := range trades {
		div := pcommon.TimeUnit(0).Add(pcommon.Env.MIN_TIME_FRAME)
		currentTime := trade.Timestamp
		if div > 0 {
			currentTime /= div
			currentTime *= div
		}

		aggSize := len(aggrQtyList)
		qty := pcommon.When[float64](trade.IsBuyerMaker).Then(trade.Quantity).Else(-trade.Quantity)
		if aggSize == 0 {
			aggrQtyList = append(aggrQtyList, setlib.NewQuantity(qty).ToTime(currentTime))
		} else {
			prev := aggrQtyList[aggSize-1]
			if prev.Time == currentTime {
				aggrQtyList = append(aggrQtyList, setlib.NewQuantity(qty).ToTime(currentTime))
			} else {
				volumesBucket = append(volumesBucket, aggrQtyList.Aggregate(pcommon.Env.MIN_TIME_FRAME, currentTime).(setlib.QuantityTime))
				aggrQtyList = []setlib.QuantityTime{}
			}
		}
	}
	if len(aggrQtyList) > 0 {
		volumesBucket = append(volumesBucket, aggrQtyList.Aggregate(pcommon.Env.MIN_TIME_FRAME, aggrQtyList[len(aggrQtyList)-1].Time).(setlib.QuantityTime))
	}
	return volumesBucket
}

func buildTradeParsingRunner(pair *pcommon.Pair, concernedAssets setlib.AssetStates, date string) *gorunner.Runner {
	runner := gorunner.NewRunner(TRADE_PARSING_KEY + "-" + pair.BuildSetID() + "-" + date)

	addTimeframe(runner, pcommon.Env.MIN_TIME_FRAME)
	addDate(runner, date)
	addAssetAndSetIDs(runner, concernedAssets.SetAndAssetIDs())

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

	addTradeParsingRunnerProcess(runner, pair)
	return runner
}
