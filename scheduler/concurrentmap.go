package scheduler

import (
	"errors"
	"sync"
)

type concurrentMap struct {
	urls  map[string]bool
	mutex sync.RWMutex
}

func newConcurrentMap() *concurrentMap {
	return &concurrentMap{urls: make(map[string]bool)}
}

func (cm *concurrentMap) put(url string) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	if ok := cm.urls[url]; ok {
		return errors.New("(" + url + ")is repeated")
	}
	cm.urls[url] = true
	return nil
}

func (cm *concurrentMap) getURLMap() map[string]bool {
	return cm.urls
}

func (cm *concurrentMap) len() int {
	return len(cm.urls)
}
