package engine

import (
	"os"
	setlib "pendulev2/set2"
	"time"

	util "pendulev2/util"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
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
		html := HTMLify(r)
		if html != nil {
			list = append(list, *html)
		}
	}
	return list
}

func (e *engine) AddTimeframeIndexing(state *setlib.AssetState, timeframe time.Duration) error {
	if err := state.FillDependencies(e.Sets); err != nil {
		return err
	}

	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return err
	}
	if timeframe <= pcommon.Env.MIN_TIME_FRAME {
		return util.ErrTimeframeTooSmall
	}

	r := buildTimeframeIndexingRunner(state, timeframe)
	e.Add(r)
	return nil
}

func (e *engine) AddTimeframeDeletion(state *setlib.AssetState, timeframe time.Duration) error {
	if err := state.FillDependencies(e.Sets); err != nil {
		return err
	}
	if timeframe < pcommon.Env.MIN_TIME_FRAME {
		return util.ErrTimeframeTooSmall
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
	for _, order := range p.Orders {
		err := order.Asset.FillDependencies(e.Sets)
		if err != nil {
			return err
		}
	}

	r := buildCSVBuildingRunner(p)
	e.Add(r)
	return nil
}

func (e *engine) AddStateParsing(asset *setlib.AssetState) error {
	if err := asset.FillDependencies(e.Sets); err != nil {
		return err
	}
	date, err := asset.ShouldSync()
	if err != nil {
		return err
	}
	if date == nil {
		return util.ErrAlreadySync
	}

	info, err := os.Stat(asset.SetRef.Settings.BuildArchiveFilePath(asset.Type(), *date, "zip"))
	if err != nil {
		return err
	}
	if info.ModTime().Unix() > time.Now().Add(-time.Minute).Unix() {
		return util.ErrFileIsTooRecent
	}

	r := buildStateParsingRunner(asset, *date)
	r.AddProcessCallback(func(engine *gorunner.Engine, runner *gorunner.Runner) {
		if runner.CountSteps() >= 4 && runner.GetError() == nil {
			e.RunAssetTasks(asset)
		}
	})
	e.Add(r)
	return nil
}

func (e *engine) RunAssetTasks(asset *setlib.AssetState) error {
	Engine.AddStateParsing(asset)
	tfs, err := asset.GetTimeFrameToReindex()
	if err != nil {
		log.WithFields(log.Fields{
			"address": asset.Address(),
			"error":   err.Error(),
		}).Error("Error getting time frame list")
		return err
	}
	for _, tf := range tfs {
		Engine.AddTimeframeIndexing(asset, tf)
	}
	return nil
}
