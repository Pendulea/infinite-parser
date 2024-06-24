package dtype

import (
	"errors"
	"sort"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
)

type CSVCheckListRequirement map[string]bool

func (c CSVCheckListRequirement) Columns() []string {
	result := lo.MapToSlice(c, func(k string, v bool) string {
		if v {
			return k
		}
		return ""
	})
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

type Data interface {
	CSVLine(prefix string, volumeDecimals int8, requirement CSVCheckListRequirement) []string
	ToRaw(decimals int8) []byte
	IsEmpty() bool
	Type() DataType
	GetTime() pcommon.TimeUnit
}

type DataList interface {
	Aggregate(timeframe time.Duration, newTime pcommon.TimeUnit) Data
	First() Data
	ToRaw(decimals int8) map[pcommon.TimeUnit][]byte
	Append(pt Data) DataList
	Prepend(pt Data) DataList
	Len() int
	RemoveFirstN(n int) DataList
}

type DataType int8

var UNIT_COLUNMS = []string{TIME, OPEN, HIGH, LOW, CLOSE, AVERAGE, MEDIAN, ABSOLUTE_SUM, COUNT}
var QUANTITY_COLUMNS = []string{TIME, PLUS, MINUS, PLUS_AVERAGE, MINUS_AVERAGE, PLUS_MEDIAN, MINUS_MEDIAN, PLUS_COUNT, MINUS_COUNT}
var POINT_COLUMNS = []string{TIME, VALUE}

func NewTypeTime(t DataType, value float64, valueTime pcommon.TimeUnit) Data {
	if t == UNIT {
		return NewUnit(value).ToTime(valueTime)
	}
	if t == QUANTITY {
		return NewQuantity(value).ToTime(valueTime)
	}
	if t == POINT {
		return newPoint(value).ToTime(valueTime)
	}
	return nil
}

func NewTypeTimeArray(t DataType) DataList {
	if t == UNIT {
		return UnitTimeArray{}
	}
	if t == QUANTITY {
		return QuantityTimeArray{}
	}
	if t == POINT {
		return PointTimeArray{}
	}
	return nil
}

func ParseTypeData(t DataType, d []byte, dataTime pcommon.TimeUnit) (Data, error) {
	if t == UNIT {
		return ParseRawUnit(d).ToTime(dataTime), nil
	}
	if t == QUANTITY {
		return ParseRawQuantity(d).ToTime(dataTime), nil
	}
	if t == POINT {
		p, err := ParseRawPoint(d)
		if err != nil {
			return nil, err
		}
		return p.ToTime(dataTime), nil
	}
	return nil, errors.New("unknown data type")
}

// units are data that can be aggregated around a candle (open, close, high, low, etc)
const UNIT DataType = 1

// quantities are data that can be summed up (volume, open interest, etc)
const QUANTITY DataType = 2

// points are simple data (a float64) that cannot be aggregated or summed
const POINT DataType = 3

func (d DataType) Columns() []string {
	if d == UNIT {
		return UNIT_COLUNMS
	}
	if d == QUANTITY {
		return QUANTITY_COLUMNS
	}
	if d == POINT {
		return POINT_COLUMNS
	}
	return []string{}
}

func (q DataType) Header(prefix string, requirement CSVCheckListRequirement) []string {
	list := []string{}
	for _, column := range q.Columns() {
		if requirement[column] {
			if column == VALUE {
				list = append(list, prefix)
			} else {
				list = append(list, prefix+"_"+column)
			}
		}
	}
	return list
}
