package util

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type TickerPriceResponse struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

func GetPairPrice(pair string, futures bool) (float64, error) {
	var url string
	if futures {
		url = fmt.Sprintf("https://fapi.binance.com/fapi/v1/ticker/price?symbol=%s", pair)
	} else {
		url = fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", pair)
	}

	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pair price: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 400 || resp.StatusCode == 404 {
		return 0, fmt.Errorf("pair %s not found", pair)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var tickerPriceResponse TickerPriceResponse
	err = json.NewDecoder(resp.Body).Decode(&tickerPriceResponse)
	if err != nil {
		return 0, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	if tickerPriceResponse.Price == "" {
		return 0, fmt.Errorf("internal error")
	}

	return strconv.ParseFloat(tickerPriceResponse.Price, 64)
}
