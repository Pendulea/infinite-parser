package set2

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	pcommon "github.com/pendulea/pendule-common"
)

type Quantity struct {
	Plus  float64 `json:"plus"`
	Minus float64 `json:"minus"`

	PlusAvg  float64 `json:"plus_avg"`
	MinusAvg float64 `json:"minus_avg"`

	PlusMed  float64 `json:"plus_med"`  // median
	MinusMed float64 `json:"minus_med"` // median

	PlusCount  int64 `json:"plus_count"`  // count
	MinusCount int64 `json:"minus_count"` // count
}

type QuantityTime struct {
	Quantity
	Time pcommon.TimeUnit `json:"time"`
}

type QuantityTimeArray []QuantityTime

func (a QuantityTimeArray) ToRaw(decimal int8) map[pcommon.TimeUnit][]byte {
	ret := make(map[pcommon.TimeUnit][]byte)
	for _, v := range a {
		ret[v.Time] = v.ToRaw(decimal)
	}
	return ret
}

func NewQuantity(v float64) Quantity {
	if v == 0.00 {
		return Quantity{}
	}

	ret := Quantity{}
	vAbs := math.Abs(v)

	if v > 0 {
		ret.Plus = vAbs
		ret.PlusAvg = vAbs
		ret.PlusMed = vAbs
		ret.PlusCount = 1
	} else {
		ret.Minus = vAbs
		ret.MinusAvg = vAbs
		ret.MinusMed = vAbs
		ret.MinusCount = 1
	}
	return ret
}

func AggregateQuantities(list QuantityTimeArray) Quantity {
	ret := Quantity{}

	amountsPlus := []float64{}
	amountMinus := []float64{}

	for _, q := range list {
		if q.Plus > 0 {
			ret.Plus += q.Plus
			ret.PlusCount++
			amountsPlus = append(amountsPlus, q.Plus)
		}
		if q.Minus > 0 {
			ret.Minus += q.Minus
			ret.MinusCount++
			amountMinus = append(amountMinus, q.Minus)
		}
	}
	ret.MinusAvg = pcommon.Math.SafeAverage(amountMinus)
	ret.PlusAvg = pcommon.Math.SafeAverage(amountsPlus)

	ret.PlusMed = pcommon.Math.SafeMedian(amountsPlus)
	ret.MinusMed = pcommon.Math.SafeMedian(amountMinus)
	return ret
}

func (m *Quantity) IsEmpty() bool {
	return m.MinusCount == 0 && m.PlusCount == 0
}

func parseRawQuantity(raw []byte) Quantity {
	s := string(raw)

	splited := strings.Split(s, "@")
	if len(splited) == 1 {
		v, err := strconv.ParseFloat(splited[0], 64)
		if err != nil {
			log.Fatal("Invalid float format")
		}
		return NewQuantity(v)
	}

	if len(splited) != 8 {
		log.Fatal("Invalid quantity format")
	}

	plus, _ := strconv.ParseFloat(splited[0], 64)
	minus, _ := strconv.ParseFloat(splited[1], 64)

	plusAvg, _ := strconv.ParseFloat(splited[2], 64)
	minusAvg, _ := strconv.ParseFloat(splited[3], 64)

	plusMed, _ := strconv.ParseFloat(splited[4], 64)
	minusMed, _ := strconv.ParseFloat(splited[5], 64)

	plusCount, _ := strconv.ParseInt(splited[6], 10, 64)
	minusCount, _ := strconv.ParseInt(splited[7], 10, 64)

	return Quantity{
		Plus:       plus,
		Minus:      minus,
		PlusAvg:    plusAvg,
		MinusAvg:   minusAvg,
		PlusMed:    plusMed,
		MinusMed:   minusMed,
		PlusCount:  plusCount,
		MinusCount: minusCount,
	}
}

func (q Quantity) ToRaw(decimals int8) []byte {
	if q.MinusCount+q.PlusCount == 1 {
		if q.Plus > 0 {
			return []byte(pcommon.Format.Float(q.Plus, decimals))
		}
		return []byte(pcommon.Format.Float(q.Minus*-1, decimals))
	}
	ret := fmt.Sprintf("%s@%s@%s@%s@%s@%s@%d@%d",
		pcommon.Format.Float(q.Plus, decimals), pcommon.Format.Float(q.Minus, decimals),
		pcommon.Format.Float(q.PlusAvg, decimals), pcommon.Format.Float(q.MinusAvg, decimals),
		pcommon.Format.Float(q.PlusMed, decimals), pcommon.Format.Float(q.MinusMed, decimals),
		q.PlusCount, q.MinusCount)
	return []byte(ret)
}

func (q Quantity) ToTime(time pcommon.TimeUnit) QuantityTime {
	return QuantityTime{
		Quantity: q,
		Time:     time,
	}
}
