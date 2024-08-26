package set2

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"pendulev2/util"
	"strconv"
	"strings"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"

	badger "github.com/dgraph-io/badger/v4"
)

const TOKEN_A_PRICE_KEY = "tokenAPrice"
const TOKEN_B_PRICE_KEY = "tokenBPrice"

type Set struct {
	initialized bool
	Assets      map[pcommon.AssetAddress]*AssetState
	Settings    pcommon.SetSettings
	db          *badger.DB
	cancels     []context.CancelFunc
	cache       map[string]interface{}
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

	var tokenAPrice, tokenBPrice float64
	firstInstance := false

	id := settings.IDString()
	dbPath := settings.DBPath()

	listFiles, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	//if the database does not exist
	if len(listFiles) == 0 {

		firstInstance = true
		//if the set is a binance pair
		if settings.IsBinancePair() == nil {
			symbol0 := strings.ToUpper(settings.ID[0])
			symbol1 := strings.ToUpper(settings.ID[1])

			//get the price of the first token
			price, err := util.GetPairPrice(symbol0+"USDT", true)
			if err != nil {
				return nil, err
			}
			if price == 0.00 {
				return nil, errors.New("price is 0")
			}
			tokenAPrice = price
			//get the price of the second token if it is not a stable coin
			if !strings.Contains(symbol1, "USD") {
				price, err := util.GetPairPrice(symbol1+"USDT", true)
				if err != nil {
					return nil, err
				}
				tokenBPrice = price
			} else {
				tokenBPrice = 1.00
			}
		}
	}

	options := badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	set := &Set{
		db:       db,
		Settings: settings,
		cancels:  make([]context.CancelFunc, 0),
		Assets:   make(map[pcommon.AssetAddress]*AssetState),
		cache:    make(map[string]interface{}),
	}

	if set.Settings.IsBinancePair() == nil {
		if firstInstance {
			if err := set.storePrices(tokenAPrice, tokenBPrice); err != nil {
				return nil, err
			}
		} else {
			tokenAPrice, tokenBPrice, err = set.getPrices()
			if err != nil {
				return nil, err
			}
		}
	}

	for _, assetSettings := range settings.Assets {
		if err := set.AddAsset(assetSettings); err != nil {
			fmt.Println(err)
			return nil, err
		}
	}

	logrus.WithFields(logrus.Fields{
		"symbol":           id,
		"assets":           len(settings.Settings),
		set.Settings.ID[0]: strconv.FormatFloat(tokenAPrice, 'f', -1, 64) + "$",
		set.Settings.ID[1]: strconv.FormatFloat(tokenBPrice, 'f', -1, 64) + "$",
	}).Info("initialized")

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

	settingsCopy := set.Settings.Copy()
	if set.initialized {
		setTypeBefore, err := set.Settings.GetSetType()
		if err != nil {
			return err
		}

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
	k, err := set.fetchAssetKey(address)
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
	set.Assets[address] = NewAssetState(assetConfig, newAsset, set, k)
	if set.initialized {
		set.Settings = *settingsCopy
	}

	return nil
}

func (s *Set) fetchAssetKey(address pcommon.AssetAddress) (*[2]byte, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(s.getAssetKey(address))
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

func (s *Set) storePrices(tokenA, tokenB float64) error {
	s.cache[TOKEN_A_PRICE_KEY] = tokenA
	s.cache[TOKEN_B_PRICE_KEY] = tokenB

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	prices := [16]byte{}
	copy(prices[:8], util.Float64ToBytes(tokenA))
	copy(prices[8:], util.Float64ToBytes(tokenB))

	if err := txn.Set(s.getPricesKey(), prices[:]); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *Set) getPrices() (tokenA, tokenB float64, err error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(s.getPricesKey())
	if err != nil {
		return 0, 0, err
	}

	var prices [16]byte
	if err := item.Value(func(val []byte) error {
		copy(prices[:], val)
		return nil
	}); err != nil {
		return 0, 0, err
	}
	tokenA = util.BytesToFloat64(prices[:8])
	tokenB = util.BytesToFloat64(prices[8:])

	s.cache[TOKEN_A_PRICE_KEY] = tokenA
	s.cache[TOKEN_B_PRICE_KEY] = tokenB
	return tokenA, tokenB, nil
}

func (s *Set) storeAddressKey(address pcommon.AssetAddress, assetKey [2]byte) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := txn.Set(s.getAssetKey(address), assetKey[:]); err != nil {
		return err
	}
	if err := txn.Set(s.getAddressKey(assetKey), s.getAssetKey(address)); err != nil {
		return err
	}

	if err := txn.Set(s.getLastUsedAssetKey(), assetKey[:]); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *Set) newAddressKey() (*[2]byte, error) {
	txn := s.db.NewTransaction(false)

	item, err := txn.Get(s.getLastUsedAssetKey())
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

			_, err := txn.Get(s.getAddressKey([2]byte{y, x}))
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

func (s *Set) CachedTokenAPrice() float64 {
	if s.cache[TOKEN_A_PRICE_KEY] == nil {
		log.Fatal("Token A price not cached")
	}
	return s.cache[TOKEN_A_PRICE_KEY].(float64)
}

func (s *Set) CachedTokenBPrice() float64 {
	if s.cache[TOKEN_B_PRICE_KEY] == nil {
		log.Fatal("Token B price not cached")
	}
	return s.cache[TOKEN_B_PRICE_KEY].(float64)
}
