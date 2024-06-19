package set2

import (
	"strconv"

	pcommon "github.com/pendulea/pendule-common"
)

type Point struct {
	Value float64 `json:"v"`
}

type PointTime struct {
	Point
	Time pcommon.TimeUnit `json:"time"`
}

type PointTimeArray []PointTime

func newPoint(v float64) Point {
	return Point{Value: v}
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

func (p Point) Bytes(decimals int8) []byte {
	return []byte(pcommon.Format.Float(p.Value, decimals))
}

func (p Point) ToTime(time pcommon.TimeUnit) PointTime {
	return PointTime{Point: p, Time: time}
}
