package rpc

import (
	"fmt"
	setlib "pendulev2/set2"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

func buildJSONSetList(sets *setlib.WorkingSets) ([]pcommon.SetJSON, error) {
	ret := []pcommon.SetJSON{}
	for _, set := range *sets {
		d, err := set.JSON()
		if err != nil {
			return nil, err
		}
		ret = append(ret, *d)
	}
	return ret, nil
}

// CheckCandlesExist checks if candles exist for the given date and time frame.
func (s *RPCService) GetSetList(payload pcommon.RPCRequestPayload) (*pcommon.GetSetListsResponse, error) {
	start := time.Now()

	list, err := buildJSONSetList(s.Sets)
	if err != nil {
		return nil, err
	}

	fmt.Println("GetSetLists took", time.Since(start))
	return &pcommon.GetSetListsResponse{SetList: list}, nil
}
