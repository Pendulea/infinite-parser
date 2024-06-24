package set2

import "pendulev2/dtype"

// Defaults assets list
const PRICE = "price"
const VOLUME = "volume"

const BOOK_DEPTH_P1 = "bd-p1"
const BOOK_DEPTH_P2 = "bd-p2"
const BOOK_DEPTH_P3 = "bd-p3"
const BOOK_DEPTH_P4 = "bd-p4"
const BOOK_DEPTH_P5 = "bd-p5"
const BOOK_DEPTH_M1 = "bd-m1"
const BOOK_DEPTH_M2 = "bd-m2"
const BOOK_DEPTH_M3 = "bd-m3"
const BOOK_DEPTH_M4 = "bd-m4"
const BOOK_DEPTH_M5 = "bd-m5"

const METRIC_SUM_OPEN_INTEREST = "metrics_sum_open_interest"
const METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO = "metrics_count_toptrader_long_short_ratio"
const METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO = "metrics_sum_toptrader_long_short_ratio"
const METRIC_COUNT_LONG_SHORT_RATIO = "metrics_count_long_short_ratio"
const METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO = "metrics_sum_taker_long_short_vol_ratio"

const CIRCULATING_SUPPLY = "circulating_supply"

var DEFAULT_ASSETS = map[string]AssetState{
	PRICE: newUninitalizedAssetState([2]byte{0, 0}, dtype.UNIT),

	VOLUME: newUninitalizedAssetState([2]byte{0, 1}, dtype.QUANTITY),

	BOOK_DEPTH_P1: newUninitalizedAssetState([2]byte{0, 2}, dtype.UNIT),
	BOOK_DEPTH_P2: newUninitalizedAssetState([2]byte{0, 3}, dtype.UNIT),
	BOOK_DEPTH_P3: newUninitalizedAssetState([2]byte{0, 4}, dtype.UNIT),
	BOOK_DEPTH_P4: newUninitalizedAssetState([2]byte{0, 5}, dtype.UNIT),
	BOOK_DEPTH_P5: newUninitalizedAssetState([2]byte{0, 6}, dtype.UNIT),
	BOOK_DEPTH_M1: newUninitalizedAssetState([2]byte{0, 7}, dtype.UNIT),
	BOOK_DEPTH_M2: newUninitalizedAssetState([2]byte{0, 8}, dtype.UNIT),
	BOOK_DEPTH_M3: newUninitalizedAssetState([2]byte{0, 9}, dtype.UNIT),
	BOOK_DEPTH_M4: newUninitalizedAssetState([2]byte{0, 10}, dtype.UNIT),
	BOOK_DEPTH_M5: newUninitalizedAssetState([2]byte{0, 11}, dtype.UNIT),

	METRIC_SUM_OPEN_INTEREST:                 newUninitalizedAssetState([2]byte{0, 12}, dtype.QUANTITY),
	METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO: newUninitalizedAssetState([2]byte{0, 13}, dtype.QUANTITY),
	METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO:   newUninitalizedAssetState([2]byte{0, 14}, dtype.QUANTITY),
	METRIC_COUNT_LONG_SHORT_RATIO:            newUninitalizedAssetState([2]byte{0, 15}, dtype.QUANTITY),
	METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO:    newUninitalizedAssetState([2]byte{0, 16}, dtype.QUANTITY),

	CIRCULATING_SUPPLY: newUninitalizedAssetState([2]byte{0, 17}, dtype.QUANTITY),
}
