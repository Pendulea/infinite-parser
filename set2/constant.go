package set2

import (
	pcommon "github.com/pendulea/pendule-common"
)

const MAX_CONSISTENCY_DAYS = 3

var DEFAULT_ASSETS = map[pcommon.AssetType]AssetState{
	pcommon.Asset.PRICE: newUninitalizedAssetState([2]byte{0, 0}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),

	pcommon.Asset.VOLUME: newUninitalizedAssetState([2]byte{0, 1}, pcommon.QUANTITY, MAX_CONSISTENCY_DAYS),

	pcommon.Asset.BOOK_DEPTH_P1: newUninitalizedAssetState([2]byte{0, 2}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_P2: newUninitalizedAssetState([2]byte{0, 3}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_P3: newUninitalizedAssetState([2]byte{0, 4}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_P4: newUninitalizedAssetState([2]byte{0, 5}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_P5: newUninitalizedAssetState([2]byte{0, 6}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_M1: newUninitalizedAssetState([2]byte{0, 7}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_M2: newUninitalizedAssetState([2]byte{0, 8}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_M3: newUninitalizedAssetState([2]byte{0, 9}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_M4: newUninitalizedAssetState([2]byte{0, 10}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.BOOK_DEPTH_M5: newUninitalizedAssetState([2]byte{0, 11}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),

	pcommon.Asset.METRIC_SUM_OPEN_INTEREST:                 newUninitalizedAssetState([2]byte{0, 12}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO: newUninitalizedAssetState([2]byte{0, 13}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO:   newUninitalizedAssetState([2]byte{0, 14}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.METRIC_COUNT_LONG_SHORT_RATIO:            newUninitalizedAssetState([2]byte{0, 15}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
	pcommon.Asset.METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO:    newUninitalizedAssetState([2]byte{0, 16}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),

	pcommon.Asset.CIRCULATING_SUPPLY: newUninitalizedAssetState([2]byte{0, 17}, pcommon.UNIT, MAX_CONSISTENCY_DAYS),
}
