package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/util"
)

// Re-fetches a new batch of keys every freq, swapping out the in-use set.
func (l *Load) startKeyFetcher(freq time.Duration) {
	for _ = range time.Tick(freq) {
		//log.Println("Fetching new keys...")
		l.setKeys()
	}
}

// Fetches l.sample random keys for l.collection, sorts them and overwrites (with locking) l.keys.
func (l *Load) setKeys() error {
	c := GetQuiverClient(l.server)
	r := &gen.InfoRequest{&l.collection, l.sample}

	if resp, err := c.GetInfo(r); err != nil {
		return err
	} else {
		if len(resp) < 1 || len(resp[0].RandomKeys) < 1 {
			return fmt.Errorf("Response (len %d) contained no keys!", len(resp))
		}
		sort.Sort(util.Keys(resp[0].RandomKeys))
		l.Lock()
		l.keys = resp[0].RandomKeys
		l.Unlock()
		return nil
	}
}

func (l *Load) randomKeys(count int) [][]byte {
	indexes := make([]int, count)
	l.RLock()
	for i := 0; i < count; i++ {
		indexes[i] = rand.Intn(len(l.keys))
	}
	sort.Ints(indexes)

	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		out[i] = l.keys[indexes[i]]
	}
	l.RUnlock()
	return out
}

func (l *Load) randomKey() []byte {
	l.RLock()
	defer l.RUnlock()
	return l.keys[rand.Intn(len(l.keys))]
}
