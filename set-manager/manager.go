package manager

import (
	"fmt"
	"os"
	"pendulev2/dtype"
	setlib "pendulev2/set2"
	engine "pendulev2/task-engine"
	"sync"

	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

type SetManager struct {
	sets *setlib.WorkingSets
	mu   sync.RWMutex
}

func (pm *SetManager) Add(newSet dtype.SetSettings, firstTimeAdd bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	list, err := pullListFromJSON(getJSONPath())
	if err != nil {
		return err
	}

	if firstTimeAdd {
		if newSet.IDString() == "" {
			return fmt.Errorf("set id is empty")
		}

		for _, p := range list {
			if p.IDString() == newSet.IDString() {
				return nil
			}
		}
		if err := pcommon.File.EnsureDir(newSet.DBPath()); err != nil {
			return err
		}

		if err := updateListToJSON(append(list, newSet), getJSONPath()); err != nil {
			return err
		}
	}

	set, err := pm.sets.Add(newSet)
	if err != nil {
		return err
	}

	if set != nil {
		for _, asset := range set.Assets {
			tfs, err := asset.GetTimeFrameToReindex()
			if err != nil {
				return err
			}
			for _, tf := range tfs {
				engine.Engine.AddTimeframeIndexing(asset, tf)
			}
		}
	}

	if !firstTimeAdd {
		set.RunValueLogGC()
	}

	return nil
}

func Init(activeSets *setlib.WorkingSets, initSetPath string) *SetManager {
	pm := &SetManager{
		sets: activeSets,
		mu:   sync.RWMutex{},
	}
	plp := getJSONPath()
	var errr error = nil
	var sets []dtype.SetSettings
	firstTimeAdd := false

	if _, err := os.Stat(plp); err != nil {
		if err := updateListToJSON([]dtype.SetSettings{}, plp); err != nil {
			log.Fatalf("Error creating sets.json file: %s", err)
		}
		sets, errr = pullListFromJSON(initSetPath)
		if errr != nil {
			log.Fatalf("Error reading sets: %s", errr)
		}
		firstTimeAdd = true
	} else {
		sets, errr = pullListFromJSON(plp)
		if errr != nil {
			log.Fatalf("Error reading sets: %s", errr)
		}
	}

	for _, p := range sets {
		if err := pm.Add(p, firstTimeAdd); err != nil {
			log.Fatalf("Error adding set: %s", err)
		}
	}
	log.WithFields(log.Fields{
		"path": plp,
		"sets": len(*activeSets),
	}).Info("Successfully loaded sets.json file")

	return pm
}
