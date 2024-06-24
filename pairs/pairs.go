package pairs

import (
	"errors"
	"fmt"
	"os"
	"strings"

	setlib "pendulev2/set2"
	engine "pendulev2/task-engine"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	pcommon "github.com/pendulea/pendule-common"
)

var ALLOWED_STABLE_SYMBOL_LIST = []string{"USDT", "USDC"}

type PairManager struct {
	sets *setlib.WorkingSets
	mu   sync.RWMutex
}

func (pm *PairManager) Add(pair pcommon.Pair, firstTimeAdd bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	list, err := pullListFromJSON(getJSONPath())
	if err != nil {
		return err
	}

	if firstTimeAdd {
		for _, p := range list {
			if p.BuildSetID() == pair.BuildSetID() {
				return nil
			}
		}
		if !pair.IsBinanceValid() {
			return errors.New("only allowed to use binance symbols")
		}

		if _, err := os.Stat(pair.BuildDBPath()); err != nil {
			check, err := pair.CheckBinanceSymbolWorks()
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"symbol":           pair.BuildSetID(),
					"minHistoricalDay": pair.MinHistoricalDay,
					"err":              err.Error(),
				}).Error("symbol not found on binance or min historical data not found")
				return err
			}
			if !check {
				logrus.WithFields(logrus.Fields{
					"symbol": pair.BuildSetID(),
				}).Error("symbol not found on binance")
				return errors.New("symbol not found on binance")
			}
		}
	}

	if !supportedFilter(pair) {
		return fmt.Errorf("this kind of pair is not supported")
	}

	if err := pair.ErrorFilter(ALLOWED_STABLE_SYMBOL_LIST); err != nil {
		return err
	}

	if firstTimeAdd {

		price, err := GetPairPrice(pair.Symbol0+pair.Symbol1, true)
		if err != nil && err.Error() == "pair not found" {
			if pair.Futures {
				return err
			}
			pair.HasFutures = false
		} else if err != nil {
			return err
		} else {
			pair.HasFutures = true
		}

		if !pair.HasFutures && !pair.Futures {
			price, err = GetPairPrice(pair.Symbol0+pair.Symbol1, false)
			if err != nil {
				return err
			}
		}

		if price == "" {
			return fmt.Errorf("internal error")
		}

		log.WithFields(log.Fields{
			"symbol": pair.BuildSetID(),
		}).Infof("Found current price of %s at %s", pair.Symbol0+pair.Symbol1, price)

		floatPrice, err := strconv.ParseFloat(price, 64)
		if err != nil {
			return err
		}

		decimals := 1
		for floatPrice > 0.1 {
			floatPrice /= 10
			decimals++
		}
		//Volume decimals is controled
		pair.VolumeDecimals = int8(decimals)
		//Only USDT is allowed as stable coin for now
		pair.Symbol1 = "USDT"

		fmt.Println(pair.Symbol0+pair.Symbol1, "Looking for min historical trading day...")
		v, err := FindMinHistoricalDay(&pair)
		if err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"symbol": pair.BuildSetID(),
		}).Infof("Found min historical trading day %s", v)

		if pair.HasFutures {
			fmt.Println(pair.Symbol0+pair.Symbol1, "Looking for min historical book depth day...")
			v, err := FindBookDepthMinHistoricalDay(&pair)
			if err != nil {
				return err
			}

			log.WithFields(log.Fields{
				"symbol": pair.BuildSetID(),
			}).Infof("Found min historical book depth day %s", v)

			fmt.Println(pair.Symbol0+pair.Symbol1, "Looking for min historical futures metrics day...")
			v, err = FindFuturesMetricsMinHistoricalDay(&pair)
			if err != nil {
				return err
			}

			log.WithFields(log.Fields{
				"symbol": pair.BuildSetID(),
			}).Infof("Found min historical futures metrics day %s", v)
		}

		if err := updateListToJSON(append(list, pair), getJSONPath()); err != nil {
			return err
		}
	}

	set, err := pm.sets.Add(pair)
	if err != nil {
		return err
	}

	if set != nil {
		set.Init()

		//initialize states
		for name, asset := range setlib.DEFAULT_ASSETS {
			var decimals int8 = -1
			if name == setlib.VOLUME {
				decimals = pair.VolumeDecimals
			}

			set.Assets[name] = asset.Copy(set, name, decimals)
			if name == setlib.PRICE || name == setlib.VOLUME {
				set.Assets[name].SetStart(pair.MinHistoricalDay)
			} else if strings.HasPrefix(name, "bd-") {
				set.Assets[name].SetStart(pair.MinBookDepthHistoricalDay)
			} else if strings.HasPrefix(name, "metrics") {
				set.Assets[name].SetStart(pair.MinFuturesMetricsHistoricalDay)
			} else {
				set.Assets[name].SetStart(pair.MinHistoricalDay)
			}

			tfs, err := set.Assets[name].GetTimeFrameToReindex()
			if err != nil {
				return err
			}
			for _, tf := range tfs {
				engine.Engine.AddTimeframeIndexing(set.Assets[name], tf)
			}
		}

		tradeStates := setlib.AssetStates{set.Assets[setlib.PRICE], set.Assets[setlib.VOLUME]}
		engine.Engine.AddTradeParsing(tradeStates, &pair)

		// bookDepthStates := setlib.AssetStates{set.Assets[setlib.BOOK_DEPTH_M1], set.Assets[setlib.BOOK_DEPTH_M2], set.Assets[setlib.BOOK_DEPTH_M3], set.Assets[setlib.BOOK_DEPTH_M4], set.Assets[setlib.BOOK_DEPTH_M5], set.Assets[setlib.BOOK_DEPTH_P1], set.Assets[setlib.BOOK_DEPTH_P2], set.Assets[setlib.BOOK_DEPTH_P3], set.Assets[setlib.BOOK_DEPTH_P4], set.Assets[setlib.BOOK_DEPTH_P5]}
		// engine.Engine.AddBookDepthParsing(bookDepthStates, &pair)

		// metricsState := setlib.AssetStates{set.Assets[setlib.METRIC_COUNT_LONG_SHORT_RATIO], set.Assets[setlib.METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO], set.Assets[setlib.METRIC_SUM_OPEN_INTEREST], set.Assets[setlib.METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO], set.Assets[setlib.METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO]}
		// engine.Engine.AddMetricsParsing(metricsState, &pair)

	}

	if !firstTimeAdd {
		triggerSetSyncTasks(set, pm.sets)
	}

	return nil
}

func triggerSetSyncTasks(set *setlib.Set, activeSets *setlib.WorkingSets) {

	set.RunValueLogGC()
	// tfs, err := set.GetTimeFrameToReindex()
	// if err != nil {
	// 	log.WithFields(log.Fields{
	// 		"symbol": set.ID(),
	// 		"error":  err.Error(),
	// 	}).Error("Error getting time frame list")
	// }
	// for _, timeframe := range tfs {
	// 	engine.Engine.AddTimeframeIndexing(activeSets, set.ID(), timeframe)
	// 	indicators, err := set.GetIndicatorsToReindex(timeframe)
	// 	if err != nil {
	// 		log.WithFields(log.Fields{
	// 			"symbol": set.ID(),
	// 			"error":  err.Error(),
	// 		}).Error("Error getting indicator list")
	// 	}
	// 	for _, indicator := range indicators {
	// 		engine.Engine.AddIndicatorIndexing(activeSets, set.ID(), indicator, timeframe)
	// 	}
	// }
	// indicators, err := set.GetIndicatorsToReindex(pcommon.Env.MIN_TIME_FRAME)
	// if err != nil {
	// 	log.WithFields(log.Fields{
	// 		"symbol": set.ID(),
	// 		"error":  err.Error(),
	// 	}).Error("Error getting indicator list")
	// }
	// for _, indicator := range indicators {
	// 	engine.Engine.AddIndicatorIndexing(activeSets, set.ID(), indicator, pcommon.Env.MIN_TIME_FRAME)
	// }
}

func Init(activeSets *setlib.WorkingSets, initPairPath string) *PairManager {
	pm := &PairManager{
		sets: activeSets,
		mu:   sync.RWMutex{},
	}
	plp := getJSONPath()
	var errr error = nil
	var pairs []pcommon.Pair
	firstTimeAdd := false

	if _, err := os.Stat(plp); err != nil {
		if err := updateListToJSON([]pcommon.Pair{}, plp); err != nil {
			log.Fatalf("Error creating pairs.json file: %s", err)
		}
		pairs, errr = pullListFromJSON(initPairPath)
		if errr != nil {
			log.Fatalf("Error reading pairs: %s", errr)
		}
		firstTimeAdd = true
	} else {
		pairs, errr = pullListFromJSON(plp)
		if errr != nil {
			log.Fatalf("Error reading pairs: %s", errr)
		}
	}

	for _, p := range pairs {
		if err := pm.Add(p, firstTimeAdd); err != nil {
			log.Fatalf("Error adding pair: %s", err)
		}
	}
	log.WithFields(log.Fields{
		"path":  plp,
		"pairs": len(*activeSets),
	}).Info("Successfully loaded pairs.json file")

	return pm
}
