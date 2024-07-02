package set2

import (
	"fmt"
	"sort"
	"strings"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
)

type CSVAssetOrder struct {
	Asset   *AssetState
	Columns pcommon.CSVCheckListRequirement
}

func ParseArrayOrder(sets WorkingSets, timeframe time.Duration, listRawOrders [][]string) ([]CSVAssetOrder, error) {
	orders := []CSVAssetOrder{}

	for _, orderPacked := range listRawOrders {

		if len := len(orderPacked); len < 3 {
			return nil, fmt.Errorf("order has invalid length %d", len)
		}
		assetAddress := pcommon.AssetAddress(orderPacked[0])
		assetAddressParsed, err := assetAddress.Parse()
		if err != nil {
			return nil, fmt.Errorf("asset address %s is invalid", assetAddress)
		}
		columns := orderPacked[1:]
		setID := strings.ToLower(strings.Join(assetAddressParsed.SetID, ""))
		set := sets.Find(setID)
		if set == nil {
			return nil, fmt.Errorf("set %s not found", setID)
		}
		assetState := set.Assets[assetAddress]
		if assetState == nil {
			return nil, fmt.Errorf("asset %s not found in set %s", assetAddress, setID)
		}
		if !assetState.IsTimeframeSupported(timeframe) {
			return nil, fmt.Errorf("asset %s does not support timeframe %s", assetAddress, timeframe)
		}

		requirements := pcommon.CSVCheckListRequirement{}
		for _, column := range columns {
			if lo.IndexOf(assetState.DataType().Columns(), pcommon.ColumnName(column)) == -1 {
				return nil, fmt.Errorf("asset %s does not have column %s", assetAddress, column)
			}
			requirements[pcommon.ColumnName(column)] = true
		}

		orders = append(orders, CSVAssetOrder{Asset: assetState, Columns: requirements})
	}

	orders = lo.UniqBy(orders, func(order CSVAssetOrder) string {
		return string(order.Asset.Address())
	})

	sort.Slice(orders, func(i, j int) bool {
		return orders[i].Asset.Address() < orders[j].Asset.Address()
	})

	return orders, nil
}
