package set2

type CSVCheckListRequirement map[string]bool

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
	PRICE: newUninitalizedAssetState([2]byte{0, 0}, UNIT),

	VOLUME: newUninitalizedAssetState([2]byte{0, 1}, QUANTITY),

	BOOK_DEPTH_P1: newUninitalizedAssetState([2]byte{0, 2}, UNIT),
	BOOK_DEPTH_P2: newUninitalizedAssetState([2]byte{0, 3}, UNIT),
	BOOK_DEPTH_P3: newUninitalizedAssetState([2]byte{0, 4}, UNIT),
	BOOK_DEPTH_P4: newUninitalizedAssetState([2]byte{0, 5}, UNIT),
	BOOK_DEPTH_P5: newUninitalizedAssetState([2]byte{0, 6}, UNIT),
	BOOK_DEPTH_M1: newUninitalizedAssetState([2]byte{0, 7}, UNIT),
	BOOK_DEPTH_M2: newUninitalizedAssetState([2]byte{0, 8}, UNIT),
	BOOK_DEPTH_M3: newUninitalizedAssetState([2]byte{0, 9}, UNIT),
	BOOK_DEPTH_M4: newUninitalizedAssetState([2]byte{0, 10}, UNIT),
	BOOK_DEPTH_M5: newUninitalizedAssetState([2]byte{0, 11}, UNIT),

	METRIC_SUM_OPEN_INTEREST:                 newUninitalizedAssetState([2]byte{0, 12}, QUANTITY),
	METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO: newUninitalizedAssetState([2]byte{0, 13}, QUANTITY),
	METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO:   newUninitalizedAssetState([2]byte{0, 14}, QUANTITY),
	METRIC_COUNT_LONG_SHORT_RATIO:            newUninitalizedAssetState([2]byte{0, 15}, QUANTITY),
	METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO:    newUninitalizedAssetState([2]byte{0, 16}, QUANTITY),

	CIRCULATING_SUPPLY: newUninitalizedAssetState([2]byte{0, 17}, QUANTITY),
}
