package main

import (
	"encoding/hex"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/foursquare/fsgo/report"
	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/util"
)

func renderErr(raw error) string {
	msg := raw.Error()
	switch e := raw.(type) {
	case *gen.HFileServiceException:
		msg = e.GetMessage()
	default:
	}
	return msg
}

// starts count worker processes, each with their own thttp client(s), watching the work chan.
func (l *Load) startWorkers(count int, maxQps bool) {
	for i := 0; i < count; i++ {
		go func() {
			client := GetQuiverClient(l.server)
			var diff *gen.HFileServiceClient
			if l.diffing {
				diff = GetQuiverClient(l.diff)
			}
			for {
				if !maxQps {
					<-l.work
				}
				l.sendOne(client, diff)
			}
		}()
	}
}

// Pick a random request type to generate and send.
func (l *Load) sendOne(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	i := rand.Int31n(100)
	switch {
	case i < l.mixPrefix:
		l.sendPrefixes(client, diff)
	case i < l.mixIterator:
		l.sendGetIterator(client, diff)
	case i < l.mixMulti:
		l.sendMulti(client, diff)
	default:
		l.sendSingle(client, diff)
	}

}

// Generate and send a random GetValuesSingle request.
func (l *Load) sendSingle(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	keys := l.randomKeys()
	r := &gen.SingleHFileKeyRequest{HfileName: &l.collection, SortedKeys: keys}

	before := time.Now()
	resp, err := client.GetValuesSingle(r)
	if err != nil {
		log.Println("[GetValuesSingle] Error fetching value:", renderErr(err), util.PrettyKeys(keys))
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".getValuesSingle", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesSingle(r)
		if diffErr != nil {

			log.Println("[GetValuesSingle] Error fetching diff value:", renderErr(diffErr), util.PrettyKeys(keys))
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
	keys := l.randomKeys()
	r := &gen.SingleHFileKeyRequest{HfileName: &l.collection, SortedKeys: keys}

	before := time.Now()
	resp, err := client.GetValuesMulti(r)
	if err != nil {
		log.Println("[GetValuesMulti] Error fetching value:", renderErr(err), util.PrettyKeys(keys))
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".getValuesMulti", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesMulti(r)
		if diffErr != nil {
			log.Println("[GetValuesMulti] Error fetching diff value:", renderErr(diffErr), util.PrettyKeys(keys))
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

	fullKeys := l.randomKeys()
	prefixes := make([][]byte, len(fullKeys))

	for i, v := range fullKeys {
		prefixes[i] = v[:len(v)/2]
	}
	sort.Sort(util.Keys(prefixes))
	limit := int32(10) // request a max of 10 k/v pairs
	r := &gen.PrefixRequest{HfileName: &l.collection, SortedKeys: prefixes, ValueLimit: &limit}

	before := time.Now()
	resp, err := client.GetValuesForPrefixes(r)
	if err != nil {
		log.Println("[GetValuesForPrefixes] Error fetching value:", renderErr(err), util.PrettyKeys(prefixes))
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".getValuesForPrefixes", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesForPrefixes(r)
		if diffErr != nil {
			log.Println("[GetValuesForPrefixes] Error fetching diff value:", renderErr(diffErr), util.PrettyKeys(prefixes))
		}
		report.TimeSince(l.diffRtt+".overall", beforeDiff)
		report.TimeSince(l.diffRtt+".getValuesForPrefixes", beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			report.Inc("diffs")
			report.Inc("diffs.getValuesForPrefixes")
			if resp != nil && diffResp != nil {
				log.Printf("[DIFF-GetValuesForPrefixes] req: %v\n", r)

				log.Printf("[DIFF-GetValuesForPrefixes] orig len: %d\n", len(resp.GetValues()))
				log.Printf("[DIFF-GetValuesForPrefixes] diff len: %d\n", len(diffResp.GetValues()))

				log.Printf("[DIFF-GetValuesForPrefixes] orig lastKey: %s\n", hex.EncodeToString(resp.GetLastKey()))
				log.Printf("[DIFF-GetValuesForPrefixes] diff lastKey: %s\n", hex.EncodeToString(diffResp.GetLastKey()))

				a := resp.GetValues()
				b := diffResp.GetValues()
				for k, values := range a {
					if other, ok := b[k]; !ok {
						log.Printf("[DIFF-GetValuesForPrefixes] Missing from diff: %s", hex.EncodeToString([]byte(k)))
					} else if !reflect.DeepEqual(values, other) {
						log.Printf("[DIFF-GetValuesForPrefixes] Different values for: %s", hex.EncodeToString([]byte(k)))
						log.Printf("[DIFF-GetValuesForPrefixes] orig (%d): %s", len(values), util.PrettyKeys(values))
						log.Printf("[DIFF-GetValuesForPrefixes] diff (%d): %s", len(other), util.PrettyKeys(other))
					}
				}
				for k, _ := range b {
					if _, ok := a[k]; !ok {
						log.Printf("[DIFF-GetValuesForPrefixes] Missing from orig: %s", hex.EncodeToString([]byte(k)))
					}
				}
				log.Println("\n")
			} else {
				log.Printf("[DIFF-GetValuesForPrefixes] req: %v\n \torig: %v\n\n\tdiff: %v\n", r, resp, diffResp)
			}
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
		log.Println("[GetIterator] Error fetching value:", renderErr(err), k)
	}
	report.TimeSince(l.rtt+".overall", before)
	report.TimeSince(l.rtt+".getIterator", before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetIterator(r)
		if diffErr != nil {
			log.Println("[GetIterator] Error fetching diff value:", renderErr(diffErr), k)
		}
		report.TimeSince(l.diffRtt+".overall", beforeDiff)
		report.TimeSince(l.diffRtt+".getIterator", beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			report.Inc("diffs")
			report.Inc("diffs.GetIterator")
			log.Printf("[DIFF-GetIterator] req: %v\n", r)
			log.Printf("[DIFF-GetIterator] orig (skip %d): %v\n", resp.GetSkipKeys(), resp)
			log.Printf("[DIFF-GetIterator] diff (skip %d): %v\n", diffResp.GetSkipKeys(), diffResp)
		}
	}
}
