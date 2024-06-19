package util

import (
	"errors"

	pcommon "github.com/pendulea/pendule-common"
)

func init() {
	for k, v := range CANDLE_COLUMNS_ID {
		CANDLE_COLUMNS_ID_REVERSE[v] = k
	}
}

const OPEN = "open"
const CLOSE = "close"
const HIGH = "high"
const LOW = "low"
const VOLUME_BOUGHT = "volume_bought"
const VOLUME_SOLD = "volume_sold"
const TRADE_COUNT = "trade_count"
const MEDIAN_VOLUME_BOUGHT = "median_volume_bought"
const AVERAGE_VOLUME_BOUGHT = "average_volume_bought"
const MEDIAN_VOLUME_SOLD = "median_volume_sold"
const AVERAGE_VOLUME_SOLD = "average_volume_sold"
const VWAP = "vwap"
const STANDARD_DEVIATION = "standard_deviation"
const ABSOLUTE_PRICE_SUM = "absolute_price_sum"

var CANDLE_COLUMNS = []string{

	OPEN,
	CLOSE,
	HIGH,
	LOW,

	VOLUME_BOUGHT,
	VOLUME_SOLD,

	TRADE_COUNT,

	MEDIAN_VOLUME_BOUGHT,
	AVERAGE_VOLUME_BOUGHT,
	MEDIAN_VOLUME_SOLD,
	AVERAGE_VOLUME_SOLD,
	VWAP,
	STANDARD_DEVIATION,
	ABSOLUTE_PRICE_SUM,

	// Book depth -5%
	"book_depth_m5_open",
	"book_depth_m5_close",
	"book_depth_m5_high",
	"book_depth_m5_low",
	"book_depth_m5_average",
	"book_depth_m5_median",
	"book_depth_m5_count",

	// Book depth -4%
	"book_depth_m4_open",
	"book_depth_m4_close",
	"book_depth_m4_high",
	"book_depth_m4_low",
	"book_depth_m4_average",
	"book_depth_m4_median",
	"book_depth_m4_count",

	// Book depth -3%
	"book_depth_m3_open",
	"book_depth_m3_close",
	"book_depth_m3_high",
	"book_depth_m3_low",
	"book_depth_m3_average",
	"book_depth_m3_median",
	"book_depth_m3_count",

	// Book depth -2%
	"book_depth_m2_open",
	"book_depth_m2_close",
	"book_depth_m2_high",
	"book_depth_m2_low",
	"book_depth_m2_average",
	"book_depth_m2_median",
	"book_depth_m2_count",

	// Book depth -1%
	"book_depth_m1_open",
	"book_depth_m1_close",
	"book_depth_m1_high",
	"book_depth_m1_low",
	"book_depth_m1_average",
	"book_depth_m1_median",
	"book_depth_m1_count",

	// Book depth +1%
	"book_depth_p1_open",
	"book_depth_p1_close",
	"book_depth_p1_high",
	"book_depth_p1_low",
	"book_depth_p1_average",
	"book_depth_p1_median",
	"book_depth_p1_count",

	// Book depth +2%
	"book_depth_p2_open",
	"book_depth_p2_close",
	"book_depth_p2_high",
	"book_depth_p2_low",
	"book_depth_p2_average",
	"book_depth_p2_median",
	"book_depth_p2_count",

	// Book depth +3%
	"book_depth_p3_open",
	"book_depth_p3_close",
	"book_depth_p3_high",
	"book_depth_p3_low",
	"book_depth_p3_average",
	"book_depth_p3_median",
	"book_depth_p3_count",

	// Book depth +4%
	"book_depth_p4_open",
	"book_depth_p4_close",
	"book_depth_p4_high",
	"book_depth_p4_low",
	"book_depth_p4_average",
	"book_depth_p4_median",
	"book_depth_p4_count",

	// Book depth +5%
	"book_depth_p5_open",
	"book_depth_p5_close",
	"book_depth_p5_high",
	"book_depth_p5_low",
	"book_depth_p5_average",
	"book_depth_p5_median",
	"book_depth_p5_count",

	// Metrics : open interest
	"metrics_sum_open_interest_open",
	"metrics_sum_open_interest_close",
	"metrics_sum_open_interest_high",
	"metrics_sum_open_interest_low",
	"metrics_sum_open_interest_average",
	"metrics_sum_open_interest_median",
	"metrics_sum_open_interest_count",

	// Metrics : top trader long short ratio
	"metrics_count_toptrader_long_short_ratio_open",
	"metrics_count_toptrader_long_short_ratio_close",
	"metrics_count_toptrader_long_short_ratio_high",
	"metrics_count_toptrader_long_short_ratio_low",
	"metrics_count_toptrader_long_short_ratio_average",
	"metrics_count_toptrader_long_short_ratio_median",
	"metrics_count_toptrader_long_short_ratio_count",

	// Metrics : top trader long short ratio
	"metrics_sum_toptrader_long_short_ratio_open",
	"metrics_sum_toptrader_long_short_ratio_close",
	"metrics_sum_toptrader_long_short_ratio_high",
	"metrics_sum_toptrader_long_short_ratio_low",
	"metrics_sum_toptrader_long_short_ratio_average",
	"metrics_sum_toptrader_long_short_ratio_median",
	"metrics_sum_toptrader_long_short_ratio_count",

	// Metrics : taker long short ratio
	"metrics_count_long_short_ratio_open",
	"metrics_count_long_short_ratio_close",
	"metrics_count_long_short_ratio_high",
	"metrics_count_long_short_ratio_low",
	"metrics_count_long_short_ratio_average",
	"metrics_count_long_short_ratio_median",
	"metrics_count_long_short_ratio_count",

	// Metrics : taker long short ratio
	"metrics_sum_taker_long_short_vol_ratio_open",
	"metrics_sum_taker_long_short_vol_ratio_close",
	"metrics_sum_taker_long_short_vol_ratio_high",
	"metrics_sum_taker_long_short_vol_ratio_low",
	"metrics_sum_taker_long_short_vol_ratio_average",
	"metrics_sum_taker_long_short_vol_ratio_median",
	"metrics_sum_taker_long_short_vol_ratio_count",
}

// Map of candle columns to their respective IDs
// Must be between AA and ZZ
var CANDLE_COLUMNS_ID = map[string]string{
	AVERAGE_VOLUME_BOUGHT: "AA",
	AVERAGE_VOLUME_SOLD:   "AB",
	CLOSE:                 "AC",
	HIGH:                  "AD",
	LOW:                   "AE",
	MEDIAN_VOLUME_BOUGHT:  "AF",
	MEDIAN_VOLUME_SOLD:    "AG",
	OPEN:                  "AH",
	STANDARD_DEVIATION:    "AI",
	TRADE_COUNT:           "AJ",
	VOLUME_BOUGHT:         "AK",
	VOLUME_SOLD:           "AL",
	VWAP:                  "AM",
	ABSOLUTE_PRICE_SUM:    "AN",

	//Metrics
	"metrics_sum_open_interest_open":    "VA",
	"metrics_sum_open_interest_close":   "VB",
	"metrics_sum_open_interest_high":    "VC",
	"metrics_sum_open_interest_low":     "VD",
	"metrics_sum_open_interest_average": "VE",
	"metrics_sum_open_interest_median":  "VF",
	"metrics_sum_open_interest_count":   "VG",

	"metrics_count_toptrader_long_short_ratio_open":    "VH",
	"metrics_count_toptrader_long_short_ratio_close":   "VI",
	"metrics_count_toptrader_long_short_ratio_high":    "VJ",
	"metrics_count_toptrader_long_short_ratio_low":     "VK",
	"metrics_count_toptrader_long_short_ratio_average": "VL",
	"metrics_count_toptrader_long_short_ratio_median":  "VM",
	"metrics_count_toptrader_long_short_ratio_count":   "VN",

	"metrics_sum_toptrader_long_short_ratio_open":    "VO",
	"metrics_sum_toptrader_long_short_ratio_close":   "VP",
	"metrics_sum_toptrader_long_short_ratio_high":    "VQ",
	"metrics_sum_toptrader_long_short_ratio_low":     "VR",
	"metrics_sum_toptrader_long_short_ratio_average": "VS",
	"metrics_sum_toptrader_long_short_ratio_median":  "VT",
	"metrics_sum_toptrader_long_short_ratio_count":   "VU",

	"metrics_count_long_short_ratio_open":    "VV",
	"metrics_count_long_short_ratio_close":   "VW",
	"metrics_count_long_short_ratio_high":    "VX",
	"metrics_count_long_short_ratio_low":     "VY",
	"metrics_count_long_short_ratio_average": "VZ",
	"metrics_count_long_short_ratio_median":  "WA",
	"metrics_count_long_short_ratio_count":   "WB",

	"metrics_sum_taker_long_short_vol_ratio_open":    "WC",
	"metrics_sum_taker_long_short_vol_ratio_close":   "WD",
	"metrics_sum_taker_long_short_vol_ratio_high":    "WE",
	"metrics_sum_taker_long_short_vol_ratio_low":     "WF",
	"metrics_sum_taker_long_short_vol_ratio_average": "WG",
	"metrics_sum_taker_long_short_vol_ratio_median":  "WH",
	"metrics_sum_taker_long_short_vol_ratio_count":   "WI",

	// Book depth -5%

	"book_depth_m5_average": "XA",
	"book_depth_m5_close":   "XB",
	"book_depth_m5_count":   "XC",
	"book_depth_m5_high":    "XD",
	"book_depth_m5_low":     "XE",
	"book_depth_m5_median":  "XF",
	"book_depth_m5_open":    "XG",

	// Book depth -4%
	"book_depth_m4_average": "XH",
	"book_depth_m4_close":   "XI",
	"book_depth_m4_count":   "XJ",
	"book_depth_m4_high":    "XK",
	"book_depth_m4_low":     "XL",
	"book_depth_m4_median":  "XM",
	"book_depth_m4_open":    "XN",

	// Book depth -3%
	"book_depth_m3_average": "XO",
	"book_depth_m3_close":   "XP",
	"book_depth_m3_count":   "XQ",
	"book_depth_m3_high":    "XR",
	"book_depth_m3_low":     "XS",
	"book_depth_m3_median":  "XT",
	"book_depth_m3_open":    "XU",

	// Book depth -2%
	"book_depth_m2_average": "XV",
	"book_depth_m2_close":   "XW",
	"book_depth_m2_count":   "XX",
	"book_depth_m2_high":    "XY",
	"book_depth_m2_low":     "XZ",
	"book_depth_m2_median":  "YA",
	"book_depth_m2_open":    "YB",

	// Book depth -1%
	"book_depth_m1_average": "YC",
	"book_depth_m1_close":   "YD",
	"book_depth_m1_count":   "YE",
	"book_depth_m1_high":    "YF",
	"book_depth_m1_low":     "YG",
	"book_depth_m1_median":  "YH",
	"book_depth_m1_open":    "YI",

	// Book depth +1%
	"book_depth_p1_average": "YJ",
	"book_depth_p1_close":   "YK",
	"book_depth_p1_count":   "YL",
	"book_depth_p1_high":    "YM",
	"book_depth_p1_low":     "YN",
	"book_depth_p1_median":  "YO",
	"book_depth_p1_open":    "YP",

	// Book depth +2%
	"book_depth_p2_average": "YQ",
	"book_depth_p2_close":   "YR",
	"book_depth_p2_count":   "YS",
	"book_depth_p2_high":    "YT",
	"book_depth_p2_low":     "YU",
	"book_depth_p2_median":  "YV",
	"book_depth_p2_open":    "YW",

	// Book depth +3%
	"book_depth_p3_average": "YX",
	"book_depth_p3_close":   "YY",
	"book_depth_p3_count":   "YZ",
	"book_depth_p3_high":    "ZA",
	"book_depth_p3_low":     "ZB",
	"book_depth_p3_median":  "ZC",
	"book_depth_p3_open":    "ZD",

	// Book depth +4%
	"book_depth_p4_average": "ZE",
	"book_depth_p4_close":   "ZF",
	"book_depth_p4_count":   "ZG",
	"book_depth_p4_high":    "ZH",
	"book_depth_p4_low":     "ZI",
	"book_depth_p4_median":  "ZJ",
	"book_depth_p4_open":    "ZK",

	// Book depth +5%
	"book_depth_p5_average": "ZL",
	"book_depth_p5_close":   "ZM",
	"book_depth_p5_count":   "ZN",
	"book_depth_p5_high":    "ZO",
	"book_depth_p5_low":     "ZP",
	"book_depth_p5_median":  "ZQ",
	"book_depth_p5_open":    "ZR",
}

// initialized in init()
var CANDLE_COLUMNS_ID_REVERSE = map[string]string{}

func IDsStringToCandleColumns(ids string) []string {
	ret := []string{}
	chunked := pcommon.ChunkString(ids, 2)
	for _, chunk := range chunked {
		if column, ok := CANDLE_COLUMNS_ID_REVERSE[chunk]; ok {
			ret = append(ret, column)
		}
	}
	return ret
}

func ParseBookDepthStringPercent(s string) (int, error) {
	mapping := map[string]int{
		"m5": -5, "m4": -4, "m3": -3, "m2": -2, "m1": -1,
		"p1": 1, "p2": 2, "p3": 3, "p4": 4, "p5": 5,
	}

	value, exists := mapping[s]
	if !exists {
		return 0, errors.New("invalid input string")
	}
	return value, nil
}
