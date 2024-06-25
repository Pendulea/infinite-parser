package dtype

import pcommon "github.com/pendulea/pendule-common"

type AssetJSON struct {
	ID               string              `json:"id"`
	Precision        int8                `json:"precision"`
	Type             DataType            `json:"type"`
	ConsistencyRange [2]pcommon.TimeUnit `json:"consistency_range"`
	Timeframe        int64               `json:"timeframe"` // in milliseconds
	SubAssets        []AssetJSON         `json:"sub_assets"`
}

type SetJSON struct {
	Settings SetSettings `json:"settings"`
	Size     int64       `json:"size"`
	Assets   []AssetJSON `json:"assets"`
}
