package set2

import (
	"fmt"
	"strings"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

type Dependencies []*AssetState

type AssetState struct {
	config                     pcommon.AssetStateConfig
	settings                   pcommon.AssetSettings
	consistencyMaxLookbackDays int
	readList                   *assetReadlist //timeframe list and last read

	SetRef          *Set //reference to the set
	key             *[2]byte
	DependenciesRef Dependencies
}

func (state *AssetState) Decimals() int8 {
	if state.SetRef.Settings.IsBinancePair() == nil {
		return state.config.SetUpDecimals(state.SetRef.CachedTokenAPrice(), state.SetRef.CachedTokenBPrice())
	}
	log.Fatal("No decimals set")
	return -1
}

func (state *AssetState) Settings() pcommon.AssetSettings {
	return state.settings
}

func (state *AssetState) Type() pcommon.AssetType {
	return state.config.ID
}

func (state *AssetState) DataHistoryTime0() pcommon.TimeUnit {
	t, err := pcommon.Format.StrDateToDate(state.settings.MinDataDate)
	if err != nil {
		log.Fatal(err)
	}
	return pcommon.NewTimeUnitFromTime(t)
}

func (state *AssetState) Address() pcommon.AssetAddress {
	return state.ParsedAddress().BuildAddress()
}

func (state *AssetState) ParsedAddress() pcommon.AssetAddressParsed {
	return state.settings.Address.AddSetID(state.SetRef.Settings.ID)
}

func (state *AssetState) Key() [2]byte {
	if state.key == nil {
		log.Fatal("Key not set")
	}
	return *state.key
}

func (state *AssetState) DataType() pcommon.DataType {
	return state.config.DataType
}

func (state *AssetState) JSON() (*pcommon.AssetJSON, error) {
	t0 := state.DataHistoryTime0()
	consistencies := []pcommon.Consistency{}

	maxRead := pcommon.TimeUnit(0)
	for _, v := range *state.readList.readList {
		consistency := pcommon.Consistency{
			Range:     [2]pcommon.TimeUnit{t0, t0},
			Timeframe: v.Timeframe.Milliseconds(),
			MinValue:  v.prevState.min,
			MaxValue:  v.prevState.max,
		}
		if v.Time > maxRead {
			maxRead = v.Time
		}

		tmax, err := state.GetLastConsistencyTimeCached(v.Timeframe)
		if err != nil {
			return nil, err
		}
		if tmax > t0 {
			consistency.Range[1] = tmax
		}
		consistencies = append(consistencies, consistency)
	}

	addressJSON, err := state.ParsedAddress().JSON()
	if err != nil {
		return nil, err
	}

	return &pcommon.AssetJSON{
		AddressString:              state.Address(),
		Address:                    addressJSON,
		ConsistencyMaxLookbackDays: state.consistencyMaxLookbackDays,
		Consistencies:              consistencies,
		DataType:                   state.DataType(),
		Decimals:                   state.Decimals(),
		MinDataDate:                state.settings.MinDataDate,
		LastReadTime:               maxRead,
	}, nil
}

func NewAssetState(config pcommon.AssetStateConfig, settings pcommon.AssetSettings, SetRef *Set, key *[2]byte) *AssetState {
	state := AssetState{
		config:                     config,
		settings:                   settings,
		consistencyMaxLookbackDays: MAX_CONSISTENCY_DAYS,
		SetRef:                     SetRef,
		readList:                   newReadlistSet(),
		key:                        key,
	}

	if err := state.pullReadList(); err != nil {
		log.Fatal(err)
	}

	return &state
}

func (state *AssetState) IsUnit() bool {
	return pcommon.DEFAULT_ASSETS[state.settings.Address.AssetType].DataType == pcommon.UNIT
}

func (state *AssetState) IsQuantity() bool {
	return pcommon.DEFAULT_ASSETS[state.settings.Address.AssetType].DataType == pcommon.QUANTITY
}

func (state *AssetState) IsPoint() bool {
	return pcommon.DEFAULT_ASSETS[state.settings.Address.AssetType].DataType == pcommon.POINT
}

func (state *AssetState) HasDependency(address pcommon.AssetAddress) bool {
	if !state.ParsedAddress().HasDependencies() {
		return state.Address() == address
	}
	for _, dep := range state.DependenciesRef {
		if dep.HasDependency(address) {
			return true
		}
	}
	return false
}

func (state *AssetState) PrintReadList() {
	for _, v := range *state.readList.readList {
		fmt.Println(v.Timeframe, v.Time.ToTime())
	}
	fmt.Println()
	fmt.Println()
}

func (state *AssetState) FillDependencies(activeSets *WorkingSets) error {
	depsSynchronized := len(state.settings.Address.Dependencies) == len(state.DependenciesRef)

	if !depsSynchronized {
		refs := make(Dependencies, len(state.settings.Address.Dependencies))
		for i, dep := range state.settings.Address.Dependencies {
			p, err := dep.Parse()
			if err != nil {
				return err
			}
			setID := p.IDString()
			set := activeSets.Find(setID)
			if set == nil {
				return fmt.Errorf("set %s not found", setID)
			}
			depAsset := set.Assets[dep]
			if depAsset == nil {
				return fmt.Errorf("asset %s not found", dep)
			}
			refs[i] = depAsset
		}
		state.DependenciesRef = refs
	}

	if depsSynchronized && state.settings.MinDataDate == "" {
		min := "2099-12-31"
		for _, dep := range state.DependenciesRef {
			if dep.settings.MinDataDate == "" {
				err := dep.FillDependencies(activeSets)
				if err != nil {
					return err
				}
			}
			if strings.Compare(min, dep.settings.MinDataDate) > 0 {
				min = dep.settings.MinDataDate
			}
		}
		if min == "2099-12-31" {
			return fmt.Errorf("no min date found")
		}
		state.settings.MinDataDate = min
	}

	return nil
}

/*
RollbackData deletes all data from the asset state until the given date.
toDate: the date to rollback to formatted as "YYYY-MM-DD" (The previous day at 23:59:59 will be the new consistency time)
*/
func (state *AssetState) RollbackData(toDate string, timeframe time.Duration, cb func(percent float64)) error {
	t1, err := state.GetLastConsistencyTimeCached(timeframe)
	if err != nil {
		return err
	}
	consistencyDate := pcommon.Format.FormatDateStr(t1.ToTime())
	if strings.Compare(toDate, consistencyDate) >= 0 {
		// rollback date is after last consistency date
		return nil
	}

	t0, err := pcommon.Format.StrDateToDate(toDate)
	if err != nil {
		return err
	}

	diff := t1 - pcommon.NewTimeUnitFromTime(t0)

	lastSend := time.Now()
	_, err = state.rollback(timeframe, toDate, func(lastT pcommon.TimeUnit, total int) {
		if time.Since(lastSend) >= time.Second*2 {
			n := lastT - pcommon.NewTimeUnitFromTime(t0)
			cb(100 - ((float64(n) / float64(diff)) * 100))
			lastSend = time.Now()
		}
	})
	return err
}
