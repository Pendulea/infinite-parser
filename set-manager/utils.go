package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"pendulev2/dtype"

	pcommon "github.com/pendulea/pendule-common"
)

func getJSONPath() string {
	return filepath.Join(pcommon.Env.DATABASES_DIR, "_sets.json")
}

func pullListFromJSON(setsPath string) ([]dtype.SetSettings, error) {
	data, err := os.ReadFile(setsPath)
	if err != nil {
		return nil, err
	}

	var list []dtype.SetSettings
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("sets.json file is not a valid json file: %s", err)
	}
	return list, nil
}

func updateListToJSON(newList []dtype.SetSettings, setsPath string) error {
	data, err := json.Marshal(newList)
	if err != nil {
		return err
	}
	if err := os.WriteFile(setsPath, data, 0644); err != nil {
		return err
	}
	return nil
}
