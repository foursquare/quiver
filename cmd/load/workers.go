package main

import (
	"log"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/dt/go-metrics-reporting"
	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/util"
)

// starts count worker processes, each with their own thttp client(s), watching the work chan.
func (l *Load) startWorkers(count int) {
	for i := 0; i < count; i++ {
		go func() {
			client := GetQuiverClient(l.server)
			var diff *gen.HFileServiceClient
			if l.diff != nil && len(*l.diff) > 0 {
				diff = GetQuiverClient(*l.diff)
			}
			for {
				<-l.work
				l.sendOne(client, diff)
			}
		}()
	}
}

// Pick a random request type to generate and send.
func (l *Load) sendOne(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	i := rand.Int31n(100)
	switch {
	case i < 15:
		l.sendGetIterator(client, diff)
	// case i < 30:
	//  l.sendPrefixes(client, diff)
	case i < 50:
		l.sendMulti(client, diff)
	default:
		l.sendSingle(client, diff)
	}

}

// Generate and send a random GetValuesSingle request.
func (l *Load) sendSingle(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	numKeys := int(math.Abs(rand.ExpFloat64()*10) + 1)
	keys := l.randomKeys(numKeys)
	r := &gen.SingleHFileKeyRequest{HfileName: &l.collection, SortedKeys: keys}

	before := time.Now()
	resp, err := client.GetValuesSingle(r)
	if err != nil {
		log.Println("[GetValuesSingle] Error fetching value:", err, util.PrettyKeys(keys))
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".getValuesSingle", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesSingle(r)
		if diffErr != nil {
			log.Println("[GetValuesSingle] Error fetching diff value:", diffErr, util.PrettyKeys(keys))
		}
		report.TimeSince(l.diffRtt+".overall", beforeDiff)
		report.TimeSince(l.diffRtt+".getValuesSingle", beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			report.Inc("diffs")
			report.Inc("diffs.getValuesSingle")
			log.Printf("[DIFF-getValuesSingle] req: %v\n%s\torig (%d): %v\n\tdiff (%d): %v\n", r, util.PrettyKeys(keys), resp.GetKeyCount(), resp, diffResp.GetKeyCount(), diffResp)
		}
	}
}

// Generate and send a random GetValuesSingle request.
func (l *Load) sendMulti(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	numKeys := int(math.Abs(rand.ExpFloat64()*10) + 1)
	keys := l.randomKeys(numKeys)
	r := &gen.SingleHFileKeyRequest{HfileName: &l.collection, SortedKeys: keys}

	before := time.Now()
	resp, err := client.GetValuesMulti(r)
	if err != nil {
		log.Println("[GetValuesMulti] Error fetching value:", err, util.PrettyKeys(keys))
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".getValuesMulti", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesMulti(r)
		if diffErr != nil {
			log.Println("[GetValuesMulti] Error fetching diff value:", diffErr, util.PrettyKeys(keys))
		}
		report.TimeSince(l.diffRtt+".overall", beforeDiff)
		report.TimeSince(l.diffRtt+".getValuesMulti", beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			report.Inc("diffs")
			report.Inc("diffs.getValuesMulti")
			log.Printf("[DIFF-getValuesMulti] req: %v\n \torig: %v\n\n\tdiff: %v\n", r, resp, diffResp)
		}
	}
}

// Generate and send a random GetValuesSingle request.
func (l *Load) sendPrefixes(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	numKeys := int(math.Abs(rand.ExpFloat64()*10) + 1)
	fullKeys := l.randomKeys(numKeys)
	prefixes := make([][]byte, len(fullKeys))

	for i, v := range fullKeys {
		prefixes[i] = v[:len(v)-2]
	}
	sort.Sort(util.Keys(prefixes))
	r := &gen.PrefixRequest{HfileName: &l.collection, SortedKeys: prefixes}

	before := time.Now()
	resp, err := client.GetValuesForPrefixes(r)
	if err != nil {
		log.Println("[GetValuesForPrefixes] Error fetching value:", err, util.PrettyKeys(prefixes))
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".GetValuesForPrefixes", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesForPrefixes(r)
		if diffErr != nil {
			log.Println("[GetValuesForPrefixes] Error fetching diff value:", diffErr, util.PrettyKeys(prefixes))
		}
		report.TimeSince(l.diffRtt+".overall", beforeDiff)
		report.TimeSince(l.diffRtt+".GetValuesForPrefixes", beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			report.Inc("diffs")
			report.Inc("diffs.GetValuesForPrefixes")
			log.Printf("[DIFF-GetValuesForPrefixes] req: %v\n \torig: %v\n\n\tdiff: %v\n", r, resp, diffResp)
		}
	}
}

// Generate and send a random GetValuesSingle request.
func (l *Load) sendGetIterator(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {

	includeValues := true
	k := l.randomKey()
	lim := int32(10)
	r := &gen.IteratorRequest{HfileName: &l.collection, IncludeValues: &includeValues, LastKey: k, ResponseLimit: &lim}

	before := time.Now()
	resp, err := client.GetIterator(r)
	if err != nil {
		log.Println("[GetIterator] Error fetching value:", err, k)
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".GetIterator", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetIterator(r)
		if diffErr != nil {
			log.Println("[GetIterator] Error fetching diff value:", diffErr, k)
		}
		report.TimeSince(l.diffRtt+".overall", beforeDiff)
		report.TimeSince(l.diffRtt+".GetIterator", beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			report.Inc("diffs")
			report.Inc("diffs.GetIterator")
			log.Printf("[DIFF-GetIterator] req: %v\n", r)
			log.Printf("[DIFF-GetIterator] orig (skip %d): %v\n", resp.GetSkipKeys(), resp)
			log.Printf("[DIFF-GetIterator] diff (skip %d): %v\n", diffResp.GetSkipKeys(), diffResp)
		}
	}
}
