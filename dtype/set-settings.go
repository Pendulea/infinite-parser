package dtype

import (
	"path/filepath"
	"strings"

	pcommon "github.com/pendulea/pendule-common"
)

type SetSettings struct {
	Assets []struct {
		ID          string `json:"id"`
		MinDataDate string `json:"min_data_date"`
		Decimals    int8   `json:"decimals"`
	} `json:"assets"`
	ID       []string `json:"id"`
	Settings []struct {
		ID    string `json:"id"`
		Value int64  `json:"value"`
	} `json:"settings"`
}

func (s *SetSettings) IDString() string {
	return strings.ToLower(strings.Join(s.ID, ""))
}

func (s *SetSettings) DBPath() string {
	return filepath.Join(pcommon.Env.DATABASES_DIR, strings.ToLower(s.IDString()))
}

func (s *SetSettings) ContainsAsset(assetID string) bool {
	for _, asset := range s.Assets {
		if asset.ID == assetID {
			return true
		}
	}
	return false
}

func (s *SetSettings) SettingValue(id string) int64 {
	if s.Settings == nil {
		return 0
	}
	for _, setting := range s.Settings {
		if setting.ID == id {
			return setting.Value
		}
	}
	return 0
}
