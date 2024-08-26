package engine

import (
	"errors"
	"os"
	setlib "pendulev2/set2"
	"time"

	util "pendulev2/util"

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
		html := HTMLify(r)
		if html != nil {
			list = append(list, *html)
		}
	}
	return list
}

func (e *engine) AddTimeframeIndexing(asset *setlib.AssetState, timeframe time.Duration) error {
	if err := asset.FillDependencies(e.Sets); err != nil {
		return err
	}

	if _, err := pcommon.Format.TimeFrameToLabel(timeframe); err != nil {
		return err
	}
	if timeframe <= pcommon.Env.MIN_TIME_FRAME {
		return util.ErrTimeframeTooSmall
	}

	if asset.ParsedAddress().HasDependencies() {
		r := buildIndicatorIndexingRunner(asset, timeframe)
		e.Add(r)
		return nil
	}

	r := buildTimeframeIndexingRunner(asset, timeframe)
	e.Add(r)
	return nil
}

// func (e *engine) AddTimeframeDeletion(state *setlib.AssetState, timeframe time.Duration) error {
// 	if err := state.FillDependencies(e.Sets); err != nil {
// 		return err
// 	}
// 	if timeframe < pcommon.Env.MIN_TIME_FRAME {
// 		return util.ErrTimeframeTooSmall
// 	}

// 	r := buildTimeframeDeletionRunner(state, timeframe)
// 	e.Add(r)
// 	return nil
// }

func (e *engine) AddCSVBuilding(from int64, to int64, timeframe int64, packed [][]string) error {
	p := setlib.CSVOrderPacked{
		Header: setlib.CSVOrderHeader{
			From:      pcommon.NewTimeUnit(from),
			To:        pcommon.NewTimeUnit(to),
			Timeframe: time.Duration(timeframe) * pcommon.TIME_UNIT_DURATION,
		},
		Orders: packed,
	}
	unpacked, err := p.Unpack(*e.Sets)
	if err != nil {
		return err
	}
	for _, order := range unpacked.Orders {
		err := order.Asset.FillDependencies(e.Sets)
		if err != nil {
			return err
		}
	}
	if len(unpacked.Orders) == 0 {
		return errors.New("no orders to build")
	}

	r := buildCSVBuildingRunner(unpacked)
	e.Add(r)
	return nil
}

func (e *engine) RollBackState(state *setlib.AssetState, date string) error {
	timeframes := state.GetActiveTimeFrameList()
	for _, tf := range timeframes {
		r := buildStateRollbackRunner(state, date, tf)
		e.Add(r)
	}
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

	if asset.ParsedAddress().HasDependencies() {
		r := buildIndicatorIndexingRunner(asset, pcommon.Env.MIN_TIME_FRAME)
		r.AddProcessCallback(func(engine *gorunner.Engine, runner *gorunner.Runner) {
			if runner.CountSteps() >= 1 && runner.GetError() == nil {
				e.RunAssetTasks(asset)
			}
		})
		e.Add(r)
		return nil
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
	if err := asset.FillDependencies(e.Sets); err != nil {
		return err
	}
	Engine.AddStateParsing(asset)
	for _, tf := range asset.SetRef.GetAllAssetsTimeframes() {
		Engine.AddTimeframeIndexing(asset, tf)
	}
	return nil
}
