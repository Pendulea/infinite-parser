package set2

import (
	"fmt"
	"log"
	"math"
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
	// The precision is the number of characters after the decimal point
	return len(str) - decimalPos - 1
}

func (list UnitTimeArray) Aggregate(timeframe time.Duration, newTime pcommon.TimeUnit) Data {
	ret := UnitTime{Time: newTime}
	closes := []float64{}

	minDecimals := 0

	for i, unit := range list {
		if unit.Count == 0 || unit.Open <= 0.00 {
			continue
		}

		if i > 0 && timeframe == pcommon.Env.MIN_TIME_FRAME {
			prevPrice := list[i-1].Close
			currentPrice := unit.Close

			if prevPrice != currentPrice {
				maxPrecision := int(math.Max(float64(getPrecision(prevPrice)), float64(getPrecision(currentPrice))))
				currentAPS := decimal.NewFromFloat(ret.AbsoluteSum)
				max := decimal.NewFromFloat(math.Max(currentPrice, prevPrice))
				min := decimal.NewFromFloat(math.Min(currentPrice, prevPrice))

				priceDiff := max.Sub(min)
				newAPS := currentAPS.Add(priceDiff)

				str := newAPS.String()
				if strings.Contains(str, ".") {
					splited := strings.Split(str, ".")
					ret := splited[0]
					if maxPrecision <= len(splited[1]) {
						ret += "." + splited[1][:maxPrecision]
					} else {
						ret += "." + splited[1]
					}
					str = ret
				}
				v, err := strconv.ParseFloat(str, 64)
				if err != nil {
					log.Fatal(err)
				}
				ret.AbsoluteSum = v
			}
		} else {
			minDecimals = int(math.Max(float64(minDecimals), float64(getPrecision(unit.AbsoluteSum))))
			ret.AbsoluteSum, _ = decimal.NewFromFloat(ret.AbsoluteSum).Add(decimal.NewFromFloat(unit.AbsoluteSum)).Float64()
		}

		ret.Open = unit.Open
		ret.High = math.Max(ret.High, unit.High)
		if ret.Low <= 0.00 {
			ret.Low = unit.Low
		} else {
			ret.Low = math.Min(ret.Low, unit.Low)
		}
		ret.Close = unit.Close
		ret.Count += unit.Count
		closes = append(closes, unit.Close)
	}

	if timeframe != pcommon.Env.MIN_TIME_FRAME {
		apsString := pcommon.Format.Float(ret.AbsoluteSum, int8(minDecimals))
		newAPS, err := strconv.ParseFloat(apsString, 64)
		if err != nil {
			log.Fatal(err)
		}
		ret.AbsoluteSum = newAPS
	}

	ret.Average = pcommon.Math.SafeAverage(closes)
	ret.Median = pcommon.Math.SafeMedian(closes)
	return ret
}

func parseRawUnit(raw []byte) Unit {
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
	return []byte(fmt.Sprintf("%s@%s@%s@%s@%s@%s@%d@%s", pcommon.Format.Float(p.Open, decimals), pcommon.Format.Float(p.High, decimals), pcommon.Format.Float(p.Low, decimals), pcommon.Format.Float(p.Close, decimals), pcommon.Format.Float(p.Average, decimals), pcommon.Format.Float(p.Median, decimals), p.Count, pcommon.Format.Float(p.AbsoluteSum, decimals)))
}

func (p Unit) ToTime(time pcommon.TimeUnit) UnitTime {
	return UnitTime{
		Unit: p,
		Time: time,
	}
}

func (q UnitTime) CSVLine(prefix string, decimals int8, requirement CSVCheckListRequirement) string {
	str := ""

	if requirement[TIME] {
		if q.Time > 0 {
			if pcommon.Env.MIN_TIME_FRAME >= time.Second {
				str += strconv.FormatInt(q.Time.ToTime().Unix(), 10) + ","
			} else {
				str += q.Time.String() + ","
			}
		} else {
			str += ","
		}
	}

	if requirement[OPEN] {
		if q.Count > 1 {
			str += pcommon.Format.Float(q.Open, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[HIGH] {
		if q.Count > 1 {
			str += pcommon.Format.Float(q.High, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[LOW] {
		if q.Count > 1 {
			str += pcommon.Format.Float(q.Low, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[CLOSE] {
		if q.Count > 0 {
			str += pcommon.Format.Float(q.Close, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[AVERAGE] {
		if q.Count > 1 {
			str += pcommon.Format.Float(q.Average, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[MEDIAN] {
		if q.Count > 1 {
			str += pcommon.Format.Float(q.Median, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[ABSOLUTE_SUM] {
		if q.AbsoluteSum != 0.00 {
			str += pcommon.Format.Float(q.AbsoluteSum, decimals) + ","
		} else {
			str += ","
		}
	}

	if requirement[COUNT] {
		if q.Count > 0 {
			str += strconv.FormatInt(q.Count, 10)
		} else {
			str += ","
		}
	}

	return str
}
