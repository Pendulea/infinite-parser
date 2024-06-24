package engine

import (
	"errors"
	setlib "pendulev2/set2"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
)

var Engine *engine = nil

type engine struct {
	*gorunner.Engine
	Sets *setlib.WorkingSets
}

func (e *engine) Init(activeSets *setlib.WorkingSets) {
	if Engine == nil {
		options := gorunner.NewEngineOptions().
			SetName("Parser").
			SetMaxSimultaneousRunner(pcommon.Env.MAX_SIMULTANEOUS_PARSING)
		Engine = &engine{
			Engine: gorunner.NewEngine(options),
			Sets:   activeSets,
		}
	}
}

// func (e *engine) GetHTMLStatuses() []StatusHTML {
// 	list := []StatusHTML{}
// 	for _, r := range e.RunningRunners() {
// 		list = append(list, HTMLify(r))
// 	}
// 	return list
// }

func (e *engine) AddTimeframeIndexing(state *setlib.AssetState, timeframe time.Duration) error {
	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return err
	}
	if timeframe <= pcommon.Env.MIN_TIME_FRAME {
		return errors.New("timeframe is too small")
	}

	r := buildTimeframeIndexingRunner(state, timeframe)
	e.Add(r)
	return nil
}

func (e *engine) DeleteTimeframe(state *setlib.AssetState, timeframe time.Duration) error {
	if timeframe < pcommon.Env.MIN_TIME_FRAME {
		return errors.New("timeframe is too small")
	}

	r := buildTimeframeDeletionRunner(state, timeframe)
	e.Add(r)
	return nil
}

func (e *engine) AddBookDepthParsing(concernedStates setlib.AssetStates, pair *pcommon.Pair) error {
	date, err := e.generalDataParsingCheck(pair, concernedStates, pair.BuildBookDepthArchivesFilePath)
	if err != nil {
		return err
	}

	r := buildBookDepthParsingRunner(pair, concernedStates, date)
	r.AddProcessCallback(func(engine *gorunner.Engine, runner *gorunner.Runner) {
		if runner.CountSteps() == 1 && runner.GetError() == nil {
			e.AddBookDepthParsing(concernedStates, pair)
			// for _, asset := range set.Assets {
			// 	tfs, err := asset.GetTimeFrameToReindex()
			// 	if err != nil {
			// 		log.WithFields(log.Fields{
			// 			"set":   set.ID(),
			// 			"state": asset.ID(),
			// 			"error": err.Error(),
			// 		}).Error("Error getting time frame list")
			// 	}
			// 	for _, timeframe := range tfs {
			// 		e.AddTimeframeIndexing(asset, timeframe)
			// 	}
			// }
		}
	})
	e.Add(r)
	return nil
}

func (e *engine) AddMetricsParsing(concernedStates setlib.AssetStates, pair *pcommon.Pair) error {
	date, err := e.generalDataParsingCheck(pair, concernedStates, pair.BuildFuturesMetricsArchivesFilePath)
	if err != nil {
		return err
	}

	r := buildMetricsParsingRunner(pair, concernedStates, date)
	r.AddProcessCallback(func(engine *gorunner.Engine, runner *gorunner.Runner) {
		if runner.CountSteps() == 1 && runner.GetError() == nil {
			e.AddMetricsParsing(concernedStates, pair)
			// for _, asset := range set.Assets {
			// 	tfs, err := asset.GetTimeFrameToReindex()
			// 	if err != nil {
			// 		log.WithFields(log.Fields{
			// 			"set":   set.ID(),
			// 			"state": asset.ID(),
			// 			"error": err.Error(),
			// 		}).Error("Error getting time frame list")
			// 	}
			// 	for _, timeframe := range tfs {
			// 		e.AddTimeframeIndexing(asset, timeframe)
			// 	}
			// }
		}
	})
	e.Add(r)
	return nil
}

func (e *engine) AddCSVBuilding(packedOrder CSVBuildingOrderPacked) error {
	p, err := parsePackedOrder(*e.Sets, packedOrder)
	if err != nil {
		return err
	}
	r := buildCSVBuildingRunner(p)
	e.Add(r)
	return nil
}

func (e *engine) AddTradeParsing(concernedStates setlib.AssetStates, pair *pcommon.Pair) error {

	date, err := e.generalDataParsingCheck(pair, concernedStates, pair.BuildTradesArchivesFilePath)
	if err != nil {
		return err
	}

	r := buildTradeParsingRunner(pair, concernedStates, date)
	r.AddProcessCallback(func(engine *gorunner.Engine, runner *gorunner.Runner) {
		if runner.Size().Max() > 0 && runner.GetError() == nil {
			e.AddTradeParsing(concernedStates, pair)
			// for _, asset := range set.Assets {
			// 	tfs, err := asset.GetTimeFrameToReindex()
			// 	if err != nil {
			// 		log.WithFields(log.Fields{
			// 			"set":   set.ID(),
			// 			"state": asset.ID(),
			// 			"error": err.Error(),
			// 		}).Error("Error getting time frame list")
			// 	}
			// 	for _, timeframe := range tfs {
			// 		e.AddTimeframeIndexing(asset, timeframe)
			// 	}
			// }
		}
	})
	e.Add(r)
	return nil
}
