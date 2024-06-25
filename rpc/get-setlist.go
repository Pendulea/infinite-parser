package rpc

import (
	"fmt"
	"pendulev2/dtype"
	setlib "pendulev2/set2"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

type GetSetListsResponse struct {
	SetList []dtype.SetJSON `json:"set_list"`
}

func buildJSONSetList(sets *setlib.WorkingSets) ([]dtype.SetJSON, error) {
	ret := []dtype.SetJSON{}
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
func (s *RPCService) GetSetList(payload pcommon.RPCRequestPayload) (*GetSetListsResponse, error) {
	start := time.Now()

	list, err := buildJSONSetList(s.Sets)
	if err != nil {
		return nil, err
	}

	fmt.Println("GetSetLists took", time.Since(start))
	return &GetSetListsResponse{SetList: list}, nil
}
