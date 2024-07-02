package set2

import (
	"fmt"

	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

type AssetState struct {
	config                     pcommon.AssetStateConfig
	settings                   pcommon.AssetSettings
	consistencyMaxLookbackDays int
	readList                   *assetReadlist //timeframe list and last read

	SetRef          *Set //reference to the set
	key             *[2]byte
	DependenciesRef []*AssetState
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

func (state *AssetState) Precision() int8 {
	return state.settings.Decimals
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
	consistencies := []pcommon.Consistency{{
		Range:     [2]pcommon.TimeUnit{t0, t0},
		Timeframe: pcommon.Env.MIN_TIME_FRAME.Milliseconds(),
	}}
	tmax, err := state.GetLastConsistencyTime(pcommon.Env.MIN_TIME_FRAME)
	if err != nil {
		return nil, err
	}
	if tmax > t0 {
		consistencies[0].Range[1] = tmax
	}

	if !state.ParsedAddress().HasDependencies() && (state.IsQuantity() || state.IsUnit()) {
		for _, v := range *state.readList.readList {
			consistency := pcommon.Consistency{
				Range:     [2]pcommon.TimeUnit{t0, t0},
				Timeframe: v.Timeframe.Milliseconds(),
			}
			tmax, err := state.GetLastConsistencyTime(v.Timeframe)
			if err != nil {
				return nil, err
			}
			if tmax > t0 {
				consistency.Range[1] = tmax
			}
			consistencies = append(consistencies, consistency)
		}
	}

	return &pcommon.AssetJSON{
		AddressString:              state.Address(),
		Address:                    state.ParsedAddress(),
		ConsistencyMaxLookbackDays: state.consistencyMaxLookbackDays,
		Consistencies:              consistencies,
		DataType:                   state.DataType(),
		Decimals:                   state.Precision(),
		MinDataDate:                state.settings.MinDataDate,
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

func (state *AssetState) PrintReadList() {
	fmt.Printf("Readlist: %s of %s\n", state.Address())
	for _, v := range *state.readList.readList {
		fmt.Println(v.Timeframe, v.Time.ToTime())
	}
	fmt.Println()
	fmt.Println()
}
