package engine

import (
	"errors"
	"os"
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

func (e *engine) GetHTMLStatuses() []pcommon.StatusHTML {
	list := []pcommon.StatusHTML{}
	for _, r := range e.RunningRunners() {
		list = append(list, HTMLify(r))
	}
	return list
}

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

func (e *engine) AddCSVBuilding(packedOrder CSVBuildingOrderPacked) error {
	p, err := parsePackedOrder(*e.Sets, packedOrder)
	if err != nil {
		return err
	}
	r := buildCSVBuildingRunner(p)
	e.Add(r)
	return nil
}

func (e *engine) AddStateParsing(state *setlib.AssetState) error {
	date, err := state.ShouldSync()
	if err != nil {
		return err
	}
	if date == nil {
		return errors.New("already sync")
	}

	info, err := os.Stat(state.BuildArchiveFilePath(*date, "zip"))
	if err != nil {
		return err
	}
	if info.ModTime().Unix() > time.Now().Add(-time.Minute).Unix() {
		return errors.New("file is too recent")
	}

	r := buildStateParsingRunner(state, *date)
	r.AddProcessCallback(func(engine *gorunner.Engine, runner *gorunner.Runner) {
		if runner.Size().Max() > 0 && runner.GetError() == nil {
			e.AddStateParsing(state)
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
	return nil
}
