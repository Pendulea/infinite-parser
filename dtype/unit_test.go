package dtype

import (
	"pendulev2/util"
	"testing"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/stretchr/testify/assert"
)

func TestUnitAggregation(t *testing.T) {
	pcommon.Env.MIN_TIME_FRAME = time.Second
	t0 := pcommon.NewTimeUnit(1587607201)
	listPrices := []float64{0.01506000, 0.06300000, 0.06300000, 0.06300000, 0.06300000, 0.06300000, 0.06300000, 0.07000000, 0.06300000, 0.06300000, 0.05250000, 0.05250000, 0.05250000, 0.06000000, 0.06000000, 0.05500000, 0.05500000, 0.05500000, 0.05500000, 0.05500000, 0.05500000, 0.05900000, 0.06500000, 0.06900000, 0.07000000, 0.07280000, 0.07300000, 0.07400000, 0.07400000}

	t1 := pcommon.NewTimeUnit(1587607202)
	listPrices2 := []float64{0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06990000,
		0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06700000, 0.06701000, 0.06701000, 0.06700000, 0.06700000,
		0.06701000, 0.06700000, 0.06700000, 0.06701000, 0.06700000, 0.06700000, 0.06700000, 0.06701000, 0.06700000, 0.06700000,
		0.06700000, 0.06700000, 0.06700000}
	t2 := pcommon.NewTimeUnit(1587607203)
	listPrices3 := []float64{0.06700000, 0.06700000, 0.06980000, 0.06700000, 0.06700000, 0.06700000, 0.06100000, 0.06980000, 0.06100000, 0.06100000,
		0.06020000, 0.06000000, 0.06000000, 0.06000000, 0.06001000, 0.06000000, 0.06000000, 0.06700000, 0.06980000, 0.06990000,
		0.06997000, 0.07000000, 0.07000000, 0.07000000, 0.07000000, 0.07000000, 0.07000000, 0.07000000, 0.07000000, 0.07000000,
		0.07000000, 0.07000000, 0.07000000, 0.07000000, 0.06167000, 0.06167000}

	var units0 UnitTimeArray
	for _, price := range listPrices {
		units0 = append(units0, NewUnit(price).ToTime(t0))
	}
	agg1 := units0.Aggregate(time.Second, t0).(UnitTime)
	assert.Equal(t, 0.01506, agg1.Open, "Open should be 0.01506000")
	assert.Equal(t, 0.074, agg1.High, "High should be 0.07400000")
	assert.Equal(t, 0.01506, agg1.Low, "Low should be 0.01506000")
	assert.Equal(t, 0.074, agg1.Close, "Close should be 0.07400000")
	assert.Equal(t, 0.06046, util.RoundFloat(agg1.Average, 6), "Average should be 0.06046s")
	assert.Equal(t, 0.063, agg1.Median, "Median should be 0.06300000")
	assert.Equal(t, 0.1039, agg1.AbsoluteSum, "AbsoluteSum should be 0.0.1039")
	assert.Equal(t, len(listPrices), int(agg1.Count), "Count is wrong")

	var units1 UnitTimeArray
	for _, price := range listPrices2 {
		units1 = append(units1, NewUnit(price).ToTime(t1))
	}

	agg2 := units1.Aggregate(time.Second, t1).(UnitTime)
	assert.Equal(t, 0.067, agg2.Open, "Open should be 0.06700000")
	assert.Equal(t, 0.0699, agg2.High, "High should be 0.06990000")
	assert.Equal(t, 0.067, agg2.Low, "Low should be 0.06700000")
	assert.Equal(t, 0.067, agg2.Close, "Close should be 0.06700000")
	assert.Equal(t, 0.06703, util.RoundFloat(agg2.Average, 5), "Average should be 0.067")
	assert.Equal(t, 0.067, agg2.Median, "Median should be 0.06700000")
	assert.Equal(t, 0.00588, agg2.AbsoluteSum, "AbsoluteSum should be 0.067")
	assert.Equal(t, len(listPrices2), int(agg2.Count), "Count is wrong")

	var units2 UnitTimeArray
	for _, price := range listPrices3 {
		units2 = append(units2, NewUnit(price).ToTime(t2))
	}

	agg3 := units2.Aggregate(time.Second, t2).(UnitTime)
	assert.Equal(t, 0.067, agg3.Open, "Open should be 0.06700000")
	assert.Equal(t, 0.070, agg3.High, "High should be 0.07000000")
	assert.Equal(t, 0.0600, agg3.Low, "Low should be 0.06000000")
	assert.Equal(t, 0.06167, agg3.Close, "Close should be 0.06167000")
	assert.Equal(t, 0.06633, util.RoundFloat(agg3.Average, 5), "Average should be 0.06633")
	assert.Equal(t, 0.0684, agg3.Median, "Median should be 0.0684")
	assert.Equal(t, 0.04855, agg3.AbsoluteSum, "AbsoluteSum should be 0.1752")

	var allUnits UnitTimeArray
	allUnits = append(allUnits, agg1)
	allUnits = append(allUnits, agg2)
	allUnits = append(allUnits, agg3)

	aggAll := allUnits.Aggregate(time.Second*3, t2).(UnitTime)
	assert.Equal(t, 0.01506, aggAll.Open, "Open should be 0.01506000")
	assert.Equal(t, 0.074, aggAll.High, "High should be 0.07000000")
	assert.Equal(t, 0.01506, aggAll.Low, "Low should be 0.06000000")
	assert.Equal(t, 0.06167, aggAll.Close, "Close should be 0.06167000")
	assert.Equal(t, 0.06756, util.RoundFloat(aggAll.Average, 5), "Average should be 0.06295")
	assert.Equal(t, 0.067, aggAll.Median, "Median should be 0.06700000")
	assert.Equal(t, 0.04855+0.00588+0.1039, aggAll.AbsoluteSum, "AbsoluteSum should be 0.1752")

	data := aggAll.ToRaw(5)
	newUnit := ParseRawUnit(data)

	assert.Equal(t, aggAll.Open, newUnit.Open, "Open should be 0.01506000")
	assert.Equal(t, aggAll.High, newUnit.High, "High should be 0.07000000")
	assert.Equal(t, aggAll.Low, newUnit.Low, "Low should be 0.06000000")
	assert.Equal(t, aggAll.Close, newUnit.Close, "Close should be 0.06167000")
	assert.Equal(t, aggAll.Average, newUnit.Average, "Average should be 0.06295")
	assert.Equal(t, aggAll.Median, newUnit.Median, "Median should be 0.06700000")
	assert.Equal(t, aggAll.AbsoluteSum, newUnit.AbsoluteSum, "AbsoluteSum should be 0.1752")
	assert.Equal(t, aggAll.Count, newUnit.Count, "Count should be 3")
}

func TestUnitQuantity(t *testing.T) {

	listVolumes := []float64{8543.5, 632, -23562, 325, 1445, -4322, 1, -1, 2844, -0.5}
	vt := pcommon.NewTimeUnit(1587607201)
	arr := QuantityTimeArray{}
	for _, volume := range listVolumes {
		arr = append(arr, NewQuantity(volume).ToTime(vt))
	}
	agg := arr.Aggregate(time.Second, vt).(QuantityTime)
	assert.Equal(t, 13790.5, agg.Plus, "Plus should be 13790.5")
	assert.Equal(t, int64(6), agg.PlusCount, "PlusCount should be 6")
	assert.Equal(t, 27885.5, agg.Minus, "Minus should be 27885.5")
	assert.Equal(t, int64(4), agg.MinusCount, "MinusCount should be 4")
	assert.Equal(t, 2298.42, util.RoundFloat(agg.PlusAvg, 2), "PlusAvg should be 0.0")
	assert.Equal(t, 6971.375, agg.MinusAvg, "MinusAvg should be 0.0")
	assert.Equal(t, 1038.5, agg.PlusMed, "PlusMed should be 0.0")
	assert.Equal(t, 2161.5, agg.MinusMed, "MinusMed should be 0.0")

	data := agg.ToRaw(2)
	newQty := ParseRawQuantity(data)

	assert.Equal(t, agg.Plus, newQty.Plus, "Plus should be 13790.5")
	assert.Equal(t, agg.Minus, newQty.Minus, "Minus should be 27885.5")
	assert.Equal(t, newQty.PlusAvg, util.RoundFloat(agg.PlusAvg, 2), "PlusAvg should be 2298.42")
	assert.Equal(t, newQty.MinusAvg, util.RoundFloat(agg.MinusAvg, 2), "MinusAvg should be 6971.375")
	assert.Equal(t, agg.PlusMed, newQty.PlusMed, "PlusMed should be 1038.5")
	assert.Equal(t, agg.MinusMed, newQty.MinusMed, "MinusMed should be 2161.5")
	assert.Equal(t, agg.PlusCount, newQty.PlusCount, "PlusCount should be 6")
	assert.Equal(t, agg.MinusCount, newQty.MinusCount, "MinusCount should be 4")

	data2 := NewQuantity(-500)
	newQty2 := ParseRawQuantity(data2.ToRaw(2))
	assert.Equal(t, newQty2.Plus, 0.0, "Plus should be 0")
	assert.Equal(t, newQty2.Minus, 500.0, "Minus should be 500")
	assert.Equal(t, newQty2.PlusAvg, 0.0, "PlusAvg should be 0")
	assert.Equal(t, newQty2.MinusAvg, 500.0, "MinusAvg should be 500")
	assert.Equal(t, newQty2.PlusMed, 0.0, "PlusMed should be 0")
	assert.Equal(t, newQty2.MinusMed, 500.0, "MinusMed should be 500")
	assert.Equal(t, newQty2.PlusCount, int64(0), "PlusCount should be 0")
	assert.Equal(t, newQty2.MinusCount, int64(1), "MinusCount should be 1")

}
