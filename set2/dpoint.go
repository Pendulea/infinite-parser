package set2

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

func newPoint(v float64) Point {
	return Point{Value: v}
}

func (m Point) Type() DataType {
	return POINT
}

func (m Point) IsEmpty() bool {
	return m.Value == 0.00
}

func parsePoint(d []byte) (Point, error) {
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

func (m PointTime) CSVLine(prefix string, volumeDecimals int8, requirement CSVCheckListRequirement) string {
	str := ""
	if requirement[TIME] {
		if m.Time > 0 {
			if pcommon.Env.MIN_TIME_FRAME >= time.Second {
				str += strconv.FormatInt(m.Time.ToTime().Unix(), 10) + ","
			} else {
				str += m.Time.String() + ","
			}
		} else {
			str += ","
		}
	}

	if requirement[VALUE] {
		if m.Value != 0.00 {
			str += pcommon.Format.Float(m.Value, volumeDecimals)
		} else {
			str += ","
		}
	}
	return str
}
