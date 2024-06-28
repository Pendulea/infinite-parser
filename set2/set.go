package set2

import (
	"context"
	"errors"
	"log"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/sirupsen/logrus"

	badger "github.com/dgraph-io/badger/v4"
)

type Set struct {
	initialized bool
	Assets      map[pcommon.AssetType]*AssetState
	Settings    pcommon.SetSettings
	db          *badger.DB
	cancels     []context.CancelFunc
}

func (set *Set) JSON() (*pcommon.SetJSON, error) {
	json := pcommon.SetJSON{
		Settings: set.Settings,
		Size:     set.Size(),
		Assets:   make([]pcommon.AssetJSON, 0),
	}

	for _, asset := range set.Assets {
		j, err := asset.JSON(pcommon.Env.MIN_TIME_FRAME)
		if err != nil {
			return nil, err
		}
		json.Assets = append(json.Assets, *j)
	}
	return &json, nil
}

func (set *Set) Size() int64 {
	lsm, vlog := set.db.Size()
	return lsm + vlog
}

func (set *Set) RunValueLogGC() {
	for {
		err := set.db.RunValueLogGC(0.5)
		if err != nil {
			if err == badger.ErrNoRewrite {
				break
			}
			log.Printf("Error running value log GC: %v", err)
			break
		}
	}
}

func (set *Set) ID() string {
	return set.Settings.IDString()
}

func NewSet(settings pcommon.SetSettings) (*Set, error) {
	if err := pcommon.File.EnsureDir(pcommon.Env.DATABASES_DIR); err != nil {
		return nil, err
	}

	id := settings.IDString()
	dbPath := settings.DBPath()

	options := badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"symbol": id,
	}).Info("DB open")

	set := &Set{
		db:       db,
		Settings: settings,
		cancels:  make([]context.CancelFunc, 0),
	}

	set.Assets = make(map[pcommon.AssetType]*AssetState)
	for _, asset := range settings.Assets {
		a, ok := DEFAULT_ASSETS[asset.ID]
		if !ok {
			return nil, errors.New("Unknown asset: " + string(asset.ID))
		}
		set.Assets[asset.ID] = a.Copy(set, asset.MinDataDate, asset.ID, asset.Decimals)
	}

	set.initialized = true
	return set, nil
}

func (s *Set) Close() {
	for _, cancel := range s.cancels {
		cancel()
	}

	if s.db != nil {
		logrus.WithFields(logrus.Fields{
			"symbol": s.ID(),
		}).Warn("Closing DB...")
		if err := s.db.Close(); err != nil {
			logrus.WithFields(logrus.Fields{
				"symbol": s.ID(),
				"msg":    err.Error(),
			}).Error("Error closing database connection")
		}
	}
}

func (s *Set) AddTimeframe(timeframe time.Duration, engineCB func(state *AssetState, timeframe time.Duration) error) {
	for _, asset := range s.Assets {
		engineCB(asset, timeframe)
	}
}

func (s *Set) RemoveTimeframe(timeframe time.Duration, engineCB func(state *AssetState, timeframe time.Duration) error) {
	for _, asset := range s.Assets {
		engineCB(asset, timeframe)
	}
}

func (s *Set) AddCancelFunc(cancel context.CancelFunc) {
	s.cancels = append(s.cancels, cancel)
}
