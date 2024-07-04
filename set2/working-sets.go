package set2

import (
	"sync"

	pcommon "github.com/pendulea/pendule-common"
)

type WorkingSets map[string]*Set

var mu = sync.RWMutex{}

func (s *WorkingSets) Find(id string) *Set {
	mu.RLock()
	defer mu.RUnlock()
	v, exist := (*s)[id]
	if !exist {
		return nil
	}
	return v
}

func (s *WorkingSets) Range() []*Set {
	mu.RLock()
	defer mu.RUnlock()
	list := make([]*Set, 0, len(*s))
	for _, v := range *s {
		list = append(list, v)
	}
	return list
}

func (s *WorkingSets) Add(setting pcommon.SetSettings) (*Set, error) {
	mu.Lock()
	defer mu.Unlock()
	id := setting.IDString()

	_, exist := (*s)[id]
	if exist {
		return nil, nil
	}

	set, err := NewSet(setting)
	if err != nil {
		return nil, err
	}

	if set == nil {
		return nil, nil
	}

	(*s)[id] = set

	return set, nil
}

func (s *WorkingSets) Remove(id string) {
	mu.Lock()
	defer mu.Unlock()

	delete(*s, id)
}

func (s *WorkingSets) StopAll() {
	mu.Lock()
	defer mu.Unlock()
	for _, set := range *s {
		set.Close()
	}
}
