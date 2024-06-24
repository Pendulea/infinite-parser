package set2

import (
	"fmt"
	"pendulev2/dtype"

	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

type AssetState struct {
	key       [2]byte
	id        string
	t         dtype.DataType
	precision int8             //precision of the data
	readList  *assetReadlist   //timeframe list and last read
	start     pcommon.TimeUnit //data start time

	SetRef *Set //reference to the set
}

func (state *AssetState) SetAndAssetID() string {
	return state.SetRef.ID() + ":" + state.ID()
}

func (state *AssetState) Precision() int8 {
	return state.precision
}

func (state *AssetState) Key() [2]byte {
	return state.key
}

func (state *AssetState) ID() string {
	return state.id
}

func (state *AssetState) Type() dtype.DataType {
	return state.t
}

func (state *AssetState) DataT0() pcommon.TimeUnit {
	return state.start
}

func (state *AssetState) Copy(SetRef *Set, minDataDate string, id string, precision int8) *AssetState {
	t, err := pcommon.Format.StrDateToDate(minDataDate)
	if err != nil {
		log.Fatal(err)
	}
	start := pcommon.NewTimeUnitFromTime(t)

	newState := AssetState{
		key:       state.key,
		t:         state.t,
		start:     start,
		id:        id,
		SetRef:    SetRef,
		readList:  newReadlistSet(),
		precision: precision,
	}

	if err := newState.pullReadList(); err != nil {
		log.Fatal(err)
	}
	return &newState
}

func newUninitalizedAssetState(key [2]byte, dataType dtype.DataType) AssetState {
	return AssetState{
		key:      key,
		t:        dataType,
		SetRef:   nil,
		readList: newReadlistSet(),
	}
}

func (state *AssetState) IsUnit() bool {
	return state.t == dtype.UNIT
}

func (state *AssetState) IsQuantity() bool {
	return state.t == dtype.QUANTITY
}

func (state *AssetState) IsPoint() bool {
	return state.t == dtype.POINT
}

func (state *AssetState) PrintReadList() {
	fmt.Printf("Readlist: %s of %s\n", state.ID(), state.SetRef.ID())
	for _, v := range *state.readList.readList {
		fmt.Println(v.Timeframe, v.Time.ToTime())
	}
	fmt.Println()
	fmt.Println()
}

func (state *AssetState) BuildArchiveFolderPath() string {
	return fmt.Sprintf("%s/%s/%s", pcommon.Env.ARCHIVES_DIR, state.SetRef.ID(), state.ID())
}

func (state *AssetState) BuildArchiveFilePath(date string, ext string) string {
	return fmt.Sprintf("%s/%s.%s", state.BuildArchiveFolderPath(), date, ext)
}
