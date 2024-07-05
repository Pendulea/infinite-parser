package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pcommon "github.com/pendulea/pendule-common"
)

func GetJSONPath() string {
	return filepath.Join(pcommon.Env.DATABASES_DIR, "_sets.json")
}

func PullListFromJSON(setsPath string) ([]pcommon.SetSettings, error) {
	data, err := os.ReadFile(setsPath)
	if err != nil {
		return nil, err
	}

	var list []pcommon.SetSettings
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("sets.json file is not a valid json file: %s", err)
	}
	for _, set := range list {
		if err := set.IsValid(); err != nil {
			return nil, err
		}
	}

	return list, nil
}

func UpdateListToJSON(newList []pcommon.SetSettings) error {
	data, err := json.Marshal(newList)
	if err != nil {
		return err
	}
	if err := os.WriteFile(GetJSONPath(), data, 0644); err != nil {
		return err
	}
	return nil
}
