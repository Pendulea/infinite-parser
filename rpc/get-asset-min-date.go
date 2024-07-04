package rpc

import (
	"errors"
	"fmt"
	"net/http"
	"pendulev2/util"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
)

type GetAssetMinDateRequest struct {
	SetID     string            `json:"set_id"`
	AssetType pcommon.AssetType `json:"asset_type"`
}

type GetAssetMinDateResponse struct {
	Date string `json:"date"`
}

func (s *RPCService) GetAssetMinDate(payload pcommon.RPCRequestPayload) (*GetAssetMinDateResponse, error) {
	r := GetAssetMinDateRequest{}
	err := pcommon.Format.DecodeMapIntoStruct(payload, &r)
	if err != nil {
		return nil, err
	}
	set := s.Sets.Find(r.SetID)
	if set == nil {
		return nil, util.ErrSetNotFound
	}

	a := r.AssetType.GetRequiredArchiveType()
	if a == nil {
		return nil, errors.New("not implemented")
	}

	similarAssets := a.GetTargetedAssets()
	for _, asset := range set.Assets {
		if lo.IndexOf(similarAssets, asset.ParsedAddress().AssetType) != -1 {
			minDataDate := asset.Settings().MinDataDate
			if minDataDate != "" {
				return &GetAssetMinDateResponse{Date: minDataDate}, nil
			}
		}
	}

	d, err := FindMinHistoricalDay(a, "2017-01-01", set.Settings)
	if err != nil {
		return nil, err
	}

	return &GetAssetMinDateResponse{Date: d}, nil
}

func FindMinHistoricalDay(t *pcommon.ArchiveType, minDateEver string, settings pcommon.SetSettings) (string, error) {
	// Set the initial dates
	startDate, err := pcommon.Format.StrDateToDate(minDateEver)
	if err != nil {
		return "", err
	}

	endDate := time.Now()

	// Initialize the result with an empty string
	var result string

	for startDate.Before(endDate) {
		midDate := startDate.Add(endDate.Sub(startDate) / 2)
		if pcommon.Format.FormatDateStr(midDate) == result {
			return result, nil
		}

		url, err := t.GetURL(pcommon.Format.FormatDateStr(midDate), settings)
		if err != nil {
			return "", err
		}

		resp, err := http.Head(url) // Perform a HEAD request
		if err != nil {
			return "", err
		}
		resp.Body.Close() // Ensure we close the response body

		log.WithFields(log.Fields{
			"set_id":  strings.Join(settings.ID, ""),
			"archive": *t,
			"date":    pcommon.Format.FormatDateStr(midDate),
			"status":  resp.Status,
		}).Info("checking date...")

		if resp.StatusCode == 200 {
			// If the URL exists, it means data is available from this date
			result = pcommon.Format.FormatDateStr(midDate)
			endDate = midDate
		} else {
			// If the URL does not exist, search later dates
			startDate = midDate.Add(time.Hour * 24)
		}

		time.Sleep(time.Millisecond * 30)
	}

	if result == "" {
		return "", fmt.Errorf("no data found")
	}

	return result, nil
}
