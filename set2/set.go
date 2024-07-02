package set2

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"

	badger "github.com/dgraph-io/badger/v4"
)

type Set struct {
	initialized bool
	Assets      map[pcommon.AssetAddress]*AssetState
	Settings    pcommon.SetSettings
	db          *badger.DB
	cancels     []context.CancelFunc
}

func (set *Set) JSON() (*pcommon.SetJSON, error) {
	t, err := set.Settings.GetSetType()
	if err != nil {
		return nil, err
	}

	json := pcommon.SetJSON{
		Settings: set.Settings,
		Size:     set.Size(),
		Assets:   make([]pcommon.AssetJSON, 0),
		Type:     t,
	}

	for _, asset := range set.Assets {
		j, err := asset.JSON()
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
		Assets:   make(map[pcommon.AssetAddress]*AssetState),
	}

	for _, assetSettings := range settings.Assets {
		if err := set.AddAsset(assetSettings); err != nil {
			return nil, err
		}
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

func (set *Set) AddAsset(newAsset pcommon.AssetSettings) error {
	if err := newAsset.IsValid(set.Settings); err != nil {
		return err
	}

	if set.initialized {
		setTypeBefore, err := set.Settings.GetSetType()
		if err != nil {
			return err
		}

		settingsCopy := set.Settings.Copy()
		settingsCopy.Assets = append(settingsCopy.Assets, newAsset)

		setTypeAfter, err := settingsCopy.GetSetType()
		if err != nil {
			return err
		}
		if setTypeAfter != setTypeBefore {
			return errors.New("asset type is not supported by set")
		}
	}

	address := newAsset.Address.AddSetID(set.Settings.ID).BuildAddress()
	k, err := set.getAddressKey(address)
	if err != nil {
		return err
	}
	if k == nil {
		k, err = set.newAddressKey()
		if err != nil {
			return err
		}
		if err := set.storeAddressKey(address, *k); err != nil {
			return err
		}
	}
	assetConfig := pcommon.DEFAULT_ASSETS[newAsset.Address.AssetType]
	fmt.Println(address, *k, assetConfig.DataType)
	set.Assets[address] = NewAssetState(assetConfig, newAsset, set, k)
	return nil
}

func (s *Set) getAddressKey(address pcommon.AssetAddress) (*[2]byte, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(address))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	var key [2]byte
	if err := item.Value(func(val []byte) error {
		copy(key[:], val)
		return nil
	}); err != nil {
		return nil, err
	}
	return &key, nil
}

func (s *Set) storeAddressKey(address pcommon.AssetAddress, key [2]byte) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := txn.Set([]byte(address), key[:]); err != nil {
		return err
	}
	reversedKey := append([]byte(string("key")), key[:]...)
	if err := txn.Set(reversedKey, []byte(address)); err != nil {
		return err
	}

	if err := txn.Set([]byte("last_key"), key[:]); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *Set) newAddressKey() (*[2]byte, error) {
	k := []byte("last_key")
	txn := s.db.NewTransaction(false)

	item, err := txn.Get(k)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &[2]byte{0, 0}, nil
		}
	}
	var key [2]byte
	if err := item.Value(func(val []byte) error {
		copy(key[:], val)
		return nil
	}); err != nil {
		return nil, err
	}

	y := key[0]
	x := key[1] + 1

Main:
	for y <= 255 {
		for x <= 255 {
			reversedKey := append([]byte(string("key")), []byte{y, x}...)
			_, err := txn.Get(reversedKey)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					break Main
				}
				return nil, err
			}
		}
		if x == 255 && y == 255 {
			return nil, errors.New("no more keys available")
		}
		x = 0
		y++
	}
	r := [2]byte{y, x}
	return &r, nil
}

func (s *Set) GetAllAssetsTimeframes() []time.Duration {
	ret := []time.Duration{}

	for _, asset := range s.Assets {
		ret = append(asset.GetActiveTimeFrameList(), ret...)
	}

	return lo.Uniq(ret)
}
