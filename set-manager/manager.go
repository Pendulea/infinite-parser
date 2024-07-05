package manager

import (
	"context"
	"fmt"
	"os"
	setlib "pendulev2/set2"
	engine "pendulev2/task-engine"
	"pendulev2/util"
	"sync"
	"time"

	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

type SetManager struct {
	sets *setlib.WorkingSets
	mu   sync.RWMutex
}

func (pm *SetManager) Add(newSet pcommon.SetSettings, firstTimeAdd bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	list, err := PullListFromJSON(GetJSONPath())
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

		if err := UpdateListToJSON(append(list, newSet)); err != nil {
			return err
		}
	}

	set, err := pm.sets.Add(newSet)
	if err != nil {
		return err
	}

	if !firstTimeAdd && set != nil {
		set.RunValueLogGC()
	}

	if set != nil {
		ctx, cancel := context.WithCancel(context.Background())
		set.AddCancelFunc(cancel)
		util.ScheduleTaskEvery(ctx, time.Minute*1, func() {
			runSetTasks(set)
		})
		runSetTasks(set)
	}

	return nil
}

func Init(activeSets *setlib.WorkingSets, initSetPath string) *SetManager {
	pm := &SetManager{
		sets: activeSets,
		mu:   sync.RWMutex{},
	}
	plp := GetJSONPath()
	var errr error = nil
	var sets []pcommon.SetSettings
	firstTimeAdd := false

	if _, err := os.Stat(plp); err != nil {
		sets, errr = PullListFromJSON(initSetPath)
		if errr != nil {
			log.Fatalf("Error reading sets: %s", errr)
		}
		if err := UpdateListToJSON([]pcommon.SetSettings{}); err != nil {
			log.Fatalf("Error creating sets.json file: %s", err)
		}
		firstTimeAdd = true
	} else {
		sets, errr = PullListFromJSON(plp)
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

func runSetTasks(set *setlib.Set) {
	for _, asset := range set.Assets {
		engine.Engine.RunAssetTasks(asset)
	}
}
