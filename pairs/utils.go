package pairs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	pcommon "github.com/pendulea/pendule-common"
)

func supportedFilter(pair pcommon.Pair) bool {
	return pair.IsBinanceValid()
}

func getJSONPath() string {
	return filepath.Join(pcommon.Env.DATABASES_DIR, "_pair-list.json")
}

func pullListFromJSON(pairsPath string) ([]pcommon.Pair, error) {
	data, err := os.ReadFile(pairsPath)
	if err != nil {
		return nil, err
	}

	// Parse the JSON data into []Pair
	var pairs []pcommon.Pair
	if err := json.Unmarshal(data, &pairs); err != nil {
		return nil, fmt.Errorf("pairs.json file is not a valid json file: %s", err)
	}
	return pairs, nil
}

func updateListToJSON(newList []pcommon.Pair, pairsPath string) error {
	data, err := json.Marshal(newList)
	if err != nil {
		return err
	}
	if err := os.WriteFile(pairsPath, data, 0644); err != nil {
		return err
	}
	return nil
}

type TickerPriceResponse struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

func GetPairPrice(pair string, futures bool) (string, error) {
	var url string
	if futures {
		url = fmt.Sprintf("https://fapi.binance.com/fapi/v1/ticker/price?symbol=%s", pair)
	} else {
		url = fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", pair)
	}

	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch pair price: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 400 || resp.StatusCode == 404 {
		return "", fmt.Errorf("pair not found")
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var tickerPriceResponse TickerPriceResponse
	err = json.NewDecoder(resp.Body).Decode(&tickerPriceResponse)
	if err != nil {
		return "", fmt.Errorf("failed to parse JSON response: %w", err)
	}

	return tickerPriceResponse.Price, nil
}

func FindFuturesMetricsMinHistoricalDay(pair *pcommon.Pair) (string, error) {
	if pair.MinFuturesMetricsHistoricalDay != "" {
		return pair.MinFuturesMetricsHistoricalDay, nil
	}

	// Set the initial dates
	startDateStr := pair.MinHistoricalDay
	startDate, err := pcommon.Format.StrDateToDate(startDateStr)
	if err != nil {
		return "", err
	}

	endDate := time.Now()

	// Initialize the result with an empty string
	var result string

	for startDate.Before(endDate) {
		midDate := startDate.Add(endDate.Sub(startDate) / 2)
		if pcommon.Format.FormatDateStr(midDate) == result {
			pair.MinFuturesMetricsHistoricalDay = result
			return result, nil
		}

		url := pair.BuildBinanceFuturesMetricsArchiveURL(pcommon.Format.FormatDateStr(midDate))
		if url == "" {
			return "", errors.New("not possible to access metrics data")
		}

		resp, err := http.Head(url) // Perform a HEAD request
		if err != nil {
			return "", err
		}
		resp.Body.Close() // Ensure we close the response body

		fmt.Println(pair.Symbol0+pair.Symbol1, "metrics", pcommon.Format.FormatDateStr(midDate), resp.Status)

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
		return "", errors.New("not possible to access metrics data")
	}

	// Update pair with the found minimum historical day
	pair.MinFuturesMetricsHistoricalDay = result
	return result, nil
}

func FindBookDepthMinHistoricalDay(pair *pcommon.Pair) (string, error) {
	if pair.MinBookDepthHistoricalDay != "" {
		return pair.MinBookDepthHistoricalDay, nil
	}

	// Set the initial dates
	startDateStr := pair.MinHistoricalDay
	startDate, err := pcommon.Format.StrDateToDate(startDateStr)
	if err != nil {
		return "", err
	}

	endDate := time.Now()

	// Initialize the result with an empty string
	var result string

	for startDate.Before(endDate) {
		midDate := startDate.Add(endDate.Sub(startDate) / 2)
		if pcommon.Format.FormatDateStr(midDate) == result {
			pair.MinBookDepthHistoricalDay = result
			return result, nil
		}

		url := pair.BuildBinanceBookDepthArchiveURL(pcommon.Format.FormatDateStr(midDate))
		if url == "" {
			return "", errors.New("not possible to access book depth data")
		}

		resp, err := http.Head(url) // Perform a HEAD request
		if err != nil {
			return "", err
		}
		resp.Body.Close() // Ensure we close the response body

		fmt.Println(pair.Symbol0+pair.Symbol1, "book depth", pcommon.Format.FormatDateStr(midDate), resp.Status)

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
		return "", errors.New("not possible to access book depth data")
	}

	// Update pair with the found minimum historical day
	pair.MinBookDepthHistoricalDay = result
	return result, nil
}

func FindMinHistoricalDay(pair *pcommon.Pair) (string, error) {
	if pair.MinHistoricalDay != "" {
		return pair.MinHistoricalDay, nil
	}

	// Set the initial dates
	startDate, err := pcommon.Format.StrDateToDate("2017-08-17")
	if err != nil {
		return "", err
	}

	endDate := time.Now()

	// Initialize the result with an empty string
	var result string

	for startDate.Before(endDate) {
		midDate := startDate.Add(endDate.Sub(startDate) / 2)
		if pcommon.Format.FormatDateStr(midDate) == result {
			pair.MinHistoricalDay = result
			return result, nil
		}

		url := pair.BuildBinanceTradesArchiveURL(pcommon.Format.FormatDateStr(midDate))
		if url == "" {
			return "", errors.New("not possible to access trades data")
		}

		resp, err := http.Head(url) // Perform a HEAD request
		if err != nil {
			return "", err
		}
		resp.Body.Close() // Ensure we close the response body

		fmt.Println(pair.Symbol0+pair.Symbol1, "trades", pcommon.Format.FormatDateStr(midDate), resp.Status)

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
		return "", errors.New("not possible to access trades data")
	}

	// Update pair with the found minimum historical day
	pair.MinHistoricalDay = result
	return result, nil
}
