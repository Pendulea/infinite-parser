package dtype

import (
	"fmt"
	"math"
	"pendulev2/util"
	"strconv"
	"strings"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/shopspring/decimal"
)

const OPEN = "open"
const HIGH = "high"
const LOW = "low"
const CLOSE = "close"
const AVERAGE = "average"
const MEDIAN = "median"
const ABSOLUTE_SUM = "absolute_sum"
const COUNT = "count"

type Unit struct {
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Average     float64 `json:"average"`
	Median      float64 `json:"median"`
	AbsoluteSum float64 `json:"absolute_sum"`
	Count       int64   `json:"count"`
}

type UnitTime struct {
	Unit
	Time pcommon.TimeUnit `json:"time"`
}

type UnitTimeArray []UnitTime

func (a UnitTimeArray) ToRaw(decimal int8) map[pcommon.TimeUnit][]byte {
	ret := make(map[pcommon.TimeUnit][]byte)
	for _, v := range a {
		ret[v.Time] = v.ToRaw(decimal)
	}
	return ret
}

func (m Unit) Type() DataType {
	return UNIT
}

func (m Unit) IsEmpty() bool {
	return m.Count == 0
}

func NewUnit(v float64) Unit {
	if v == 0.00 {
		return Unit{}
	}

	return Unit{
		Open:    v,
		High:    v,
		Low:     v,
		Close:   v,
		Average: v,
		Median:  v,
		Count:   1,
	}
}

func getPrecision(val float64) int {
	// Convert the float64 to a string with high precision
	str := pcommon.Format.Float(val, -1)
	// Find the position of the decimal point
	decimalPos := strings.Index(str, ".")
	if decimalPos == -1 {
		// No decimal point found, so the precision is 0
		return 0
	}

	//trim all 0's at end
	for i := len(str) - 1; i > decimalPos; i-- {
		if str[i] != '0' {
			break
		}
		str = str[:len(str)-1]
	}

	// The precision is the number of characters after the decimal point
	return len(str) - decimalPos - 1
}

func (list UnitTimeArray) Aggregate(timeframe time.Duration, newTime pcommon.TimeUnit) Data {
	ret := UnitTime{Time: newTime}
	closes := []float64{}

	absoluteSumDecimals := 0
	absoluteSum := decimal.NewFromFloat(0.00)
	maxClosePrecision := 0

	for i, unit := range list {
		if unit.Count == 0 || unit.Open == 0.00 {
			continue
		}
		currentUnitPricision := getPrecision(unit.Close)
		if currentUnitPricision > maxClosePrecision {
			maxClosePrecision = currentUnitPricision
		}

		if i > 0 && timeframe == pcommon.Env.MIN_TIME_FRAME {
			prevValue := list[i-1].Close
			currentValue := unit.Close

			if prevValue != currentValue {
				if currentUnitPricision > absoluteSumDecimals {
					absoluteSumDecimals = currentUnitPricision
				}

				max := decimal.NewFromFloat(math.Max(currentValue, prevValue))
				min := decimal.NewFromFloat(math.Min(currentValue, prevValue))

				priceDiff := max.Sub(min)
				absoluteSum = absoluteSum.Add(priceDiff)
			}
		} else if timeframe != pcommon.Env.MIN_TIME_FRAME {
			absoluteSumDecimals = int(math.Max(float64(absoluteSumDecimals), float64(getPrecision(unit.AbsoluteSum))))
			absoluteSum = absoluteSum.Add(decimal.NewFromFloat(unit.AbsoluteSum))
		}

		if ret.Open == 0.00 {
			ret.Open = unit.Open
		}

		if ret.High == 0.00 {
			ret.High = unit.High
		} else {
			ret.High = math.Max(ret.High, unit.High)
		}
		if ret.Low == 0.00 {
			ret.Low = unit.Low
		} else {
			ret.Low = math.Min(ret.Low, unit.Low)
		}

		ret.Close = unit.Close
		ret.Count += unit.Count
		closes = append(closes, unit.Close)
	}

	ret.AbsoluteSum, _ = absoluteSum.Round(int32(absoluteSumDecimals)).Float64()
	ret.Average = util.RoundFloat(pcommon.Math.SafeAverage(closes), uint(maxClosePrecision))
	ret.Median = pcommon.Math.SafeMedian(closes)
	return ret
}

func ParseRawUnit(raw []byte) Unit {
	s := string(raw)
	splited := strings.Split(s, "@")
	if len(splited) == 1 {
		v, err := strconv.ParseFloat(splited[0], 64)
		if err != nil {
			return Unit{}
		}
		return NewUnit(v)
	}
	if len(splited) != 8 {
		return Unit{}
	}
	open, _ := strconv.ParseFloat(splited[0], 64)
	high, _ := strconv.ParseFloat(splited[1], 64)
	low, _ := strconv.ParseFloat(splited[2], 64)
	close, _ := strconv.ParseFloat(splited[3], 64)
	avg, _ := strconv.ParseFloat(splited[4], 64)
	median, _ := strconv.ParseFloat(splited[5], 64)
	absoluteSum, _ := strconv.ParseFloat(splited[6], 64)
	count, _ := strconv.ParseInt(splited[7], 10, 64)

	return Unit{
		Open:        open,
		High:        high,
		Low:         low,
		Close:       close,
		Average:     avg,
		Median:      median,
		AbsoluteSum: absoluteSum,
		Count:       count,
	}
}

func (p Unit) ToRaw(decimals int8) []byte {
	if p.Count == 1 {
		return []byte(pcommon.Format.Float(p.Open, decimals))
	}
	return []byte(fmt.Sprintf("%f@%f@%f@%f@%f@%f@%f@%d", p.Open, p.High, p.Low, p.Close, p.Average, p.Median, p.AbsoluteSum, p.Count))
}

func (p Unit) ToTime(time pcommon.TimeUnit) UnitTime {
	return UnitTime{
		Unit: p,
		Time: time,
	}
}

func (q UnitTime) CSVLine(prefix string, decimals int8, requirement CSVCheckListRequirement) []string {
	ret := []string{}

	if requirement[TIME] {
		if q.Time > 0 {
			if pcommon.Env.MIN_TIME_FRAME >= time.Second && pcommon.Env.MIN_TIME_FRAME%time.Second == 0 {
				ret = append(ret, strconv.FormatInt(q.Time.ToTime().Unix(), 10))
			} else {
				ret = append(ret, q.Time.String())
			}
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[OPEN] {
		if q.Count > 1 {
			ret = append(ret, pcommon.Format.Float(q.Open, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[HIGH] {
		if q.Count > 1 {
			ret = append(ret, pcommon.Format.Float(q.High, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[LOW] {
		if q.Count > 1 {
			ret = append(ret, pcommon.Format.Float(q.Low, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[CLOSE] {
		if q.Count > 0 {
			ret = append(ret, pcommon.Format.Float(q.Close, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[AVERAGE] {
		if q.Count > 1 {
			ret = append(ret, pcommon.Format.Float(q.Average, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[MEDIAN] {
		if q.Count > 1 {
			ret = append(ret, pcommon.Format.Float(q.Median, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[ABSOLUTE_SUM] {
		if q.AbsoluteSum != 0.00 {
			ret = append(ret, pcommon.Format.Float(q.AbsoluteSum, decimals))
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[COUNT] {
		if q.Count > 0 {
			ret = append(ret, strconv.FormatInt(q.Count, 10))
		} else {
			ret = append(ret, "")
		}
	}
	return ret
}

func (u UnitTime) String() string {
	return fmt.Sprintf("[%d] Open: %s High: %s Low: %s Close: %s Average: %s Median: %s AbsoluteSum: %s Count: %d", u.Time.ToTime().Unix(),
		pcommon.Format.Float(u.Open, -1),
		pcommon.Format.Float(u.High, -1),
		pcommon.Format.Float(u.Low, -1),
		pcommon.Format.Float(u.Close, -1),
		pcommon.Format.Float(u.Average, -1),
		pcommon.Format.Float(u.Median, -1),
		pcommon.Format.Float(u.AbsoluteSum, -1),
		u.Count)
}
