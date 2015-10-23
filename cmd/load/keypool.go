package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"

	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/util"
)

func (l *Load) pickKeyCount() int {
	return int(math.Min(math.Abs(rand.ExpFloat64()*l.keysPerReqSpread)+l.keysPerReqMin, l.keysPerReqMax))
}

// Fetches l.sample random keys for l.collection, sorts them and overwrites (with locking) l.keys.
func (l *Load) setKeys() error {
	c := GetQuiverClient(l.server)
	r := &gen.InfoRequest{&l.collection, l.sample}

	if resp, err := c.ScanCollectionAndSampleKeys(r); err != nil {
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

func (l *Load) randomKeys() [][]byte {
	count := l.pickKeyCount()
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

func (l *Load) printKeySpread() {
	seen := make(map[int]int)
	for i := 0; i < 100000; i++ {
		k := l.pickKeyCount()
		seen[k] = seen[k] + 1
	}

	var keys []int
	max := 0
	for k, v := range seen {
		keys = append(keys, k)
		if v > max {
			max = v
		}
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Println(k, "\t", seen[k], "\t", strings.Repeat("#", seen[k]*100/max))
	}
}
