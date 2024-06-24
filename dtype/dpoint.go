package dtype

import (
	"log"
	"strconv"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

const VALUE = "value"

type Point struct {
	Value float64 `json:"v"`
}

type PointTime struct {
	Point
	Time pcommon.TimeUnit `json:"time"`
}

type PointTimeArray []PointTime

func (lst PointTimeArray) Aggregate(timeframe time.Duration, newTime pcommon.TimeUnit) Data {
	log.Fatal("no aggregation for points data")
	return PointTime{}
}

func (lst PointTimeArray) Append(pt Data) DataList {
	return append(lst, pt.(PointTime))
}

func (lst PointTimeArray) Prepend(pt Data) DataList {
	return append(PointTimeArray{pt.(PointTime)}, lst...)
}

func (lst PointTimeArray) RemoveFirstN(n int) DataList {
	if n >= len(lst) {
		return PointTimeArray{}
	}
	return lst[n:]
}

func (lst PointTimeArray) First() Data {
	if len(lst) == 0 {
		return nil
	}
	return &lst[0]
}

func (lst PointTimeArray) Len() int {
	if lst == nil {
		return 0
	}
	return len(lst)
}

func newPoint(v float64) Point {
	return Point{Value: v}
}

func (m Point) Type() DataType {
	return POINT
}

func (m Point) IsEmpty() bool {
	return m.Value == 0.00
}

func ParseRawPoint(d []byte) (Point, error) {
	if len(d) == 0 {
		return Point{}, nil
	}
	v, err := strconv.ParseFloat(string(d), 64)
	if err != nil {
		return Point{}, err
	}
	return newPoint(v), nil
}

func (p Point) ToTime(time pcommon.TimeUnit) PointTime {
	return PointTime{Point: p, Time: time}
}

func (p Point) ToRaw(decimals int8) []byte {
	return []byte(pcommon.Format.Float(p.Value, decimals))
}

func (p PointTime) GetTime() pcommon.TimeUnit {
	return p.Time
}

func (m PointTime) CSVLine(prefix string, volumeDecimals int8, requirement CSVCheckListRequirement) []string {
	ret := []string{}

	if requirement[TIME] {
		if m.Time > 0 {
			if pcommon.Env.MIN_TIME_FRAME >= time.Second {
				ret = append(ret, strconv.FormatInt(m.Time.ToTime().Unix(), 10))
			} else {
				ret = append(ret, m.Time.String())
			}
		} else {
			ret = append(ret, "")
		}
	}

	if requirement[VALUE] {
		if m.Value != 0.00 {
			ret = append(ret, pcommon.Format.Float(m.Value, volumeDecimals))
		} else {
			ret = append(ret, "")
		}
	}

	return ret
}
