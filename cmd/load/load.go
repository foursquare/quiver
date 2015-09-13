package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dt/go-metrics-reporting"
	"github.com/dt/thile/client"
	"github.com/dt/thile/gen"
	"github.com/dt/thile/util"
)

type Load struct {
	collection string
	sample     *int64
	keys       [][]byte

	server string
	diff   *string // optional

	work chan bool

	rtt     report.Timer
	diffRtt report.Timer
	diffs   report.Meter // mis-matched responses.

	queueSize report.Guage
	dropped   report.Meter

	// for atomic keyset swaps in setKeys.
	sync.RWMutex
}

// Pick a random request type to generate and send.
func (l *Load) sendOne(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	// TODO: randomly change up request type generated
	l.sendSingle(client, diff)
}

// Generate and send a random GetValuesSingle request.
func (l *Load) sendSingle(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	numKeys := int(math.Abs(rand.ExpFloat64()*10) + 1)
	keys := l.randomKeys(numKeys)
	r := &gen.SingleHFileKeyRequest{HfileName: &l.collection, SortedKeys: keys}

	before := time.Now()
	resp, err := client.GetValuesSingle(r)
	if err != nil {
		log.Println("Error fetching value:", err)
	}
	l.rtt.UpdateSince(before)

	if diff != nil {
		beforeDiff := time.Now()
		diffResp, diffErr := diff.GetValuesSingle(r)
		if err != nil {
			log.Println("Error fetching diff value:", diffErr)
		}
		l.diffRtt.UpdateSince(beforeDiff)

		if err == nil && diffErr == nil && !reflect.DeepEqual(resp, diffResp) {
			l.diffs.Mark(1)
			hexKeys := make([]string, len(keys))
			for i, key := range keys {
				hexKeys[i] = hex.EncodeToString(key)
			}
			log.Printf("[DIFF] req: %v\n\t%s\n\torig (%d): %v\n\tdiff (%d): %v\n", r, strings.Join(hexKeys, "\n\t"), resp.GetKeyCount(), resp, diffResp.GetKeyCount(), diffResp)
		}
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

// Fetches l.sample random keys for l.collection, sorts them and overwrites (with locking) l.keys.
func (l *Load) setKeys() error {
	c := thttp.NewThriftHttpRpcClient(l.server)
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

// Re-fetches a new batch of keys every freq, swapping out the in-use set.
func (l *Load) startKeyFetcher(freq time.Duration) {
	for _ = range time.Tick(freq) {
		log.Println("Fetching new keys...")
		l.setKeys()
	}
}

// Feeds the work channel at requested qps.
func (l *Load) generator(qps int) {
	pause := time.Duration(time.Second.Nanoseconds() / int64(qps))

	for _ = range time.Tick(pause) {
		l.queueSize.Update(int64(len(l.work)))
		select {
		case l.work <- true:
		default:
			l.dropped.Mark(1)
		}
	}
}

// starts count worker processes, each with their own thttp client(s), watching the work chan.
func (l *Load) startWorkers(count int) {
	for i := 0; i < count; i++ {
		go func() {
			client := thttp.NewThriftHttpRpcClient(l.server)
			var diff *gen.HFileServiceClient
			if l.diff != nil && len(*l.diff) > 0 {
				diff = thttp.NewThriftHttpRpcClient(*l.diff)
			}
			for {
				<-l.work
				l.sendOne(client, diff)
			}
		}()
	}
}

// given a string like testing=fsan44:20202, return (http://fsan44:20202/rpc/HFileService, testing).
func hfileUrlAndName(s string) (string, string) {
	name := strings.NewReplacer("http://", "", ".", "_", ":", "_", "/", "_").Replace(s)

	if parts := strings.Split(s, "="); len(parts) > 1 {
		s = parts[1]
		name = parts[0]
	}

	if !strings.Contains(s, "/") {
		fmt.Printf("'%s' doens't appear to specify a path. Appending /rpc/HFileService...\n", s)
		s = s + "/rpc/HFileService"
	}

	if !strings.HasPrefix(s, "http") {
		s = "http://" + s
	}
	return s, name
}

func main() {
	orig := flag.String("server", "localhost:9999", "URL of hfile server")
	rawDiff := flag.String("diff", "", "URL of second hfile server to compare")
	collection := flag.String("collection", "", "name of collection")
	graphite := report.Flag()
	workers := flag.Int("workers", 8, "worker pool size")
	qps := flag.Int("qps", 100, "qps to attempt")
	sample := flag.Int64("sampleSize", 1000, "number of random keys to use")

	flag.Parse()

	r := report.NewRecorder().
		MaybeReportTo(graphite).
		LogToConsole(time.Second * 10).
		SetAsDefault()

	rttName := "rtt"
	server, name := hfileUrlAndName(*orig)

	if collection == nil || len(*collection) < 1 {
		fmt.Println("--collection is required")
		c := thttp.NewThriftHttpRpcClient(server)
		r := &gen.InfoRequest{}

		if resp, err := c.GetInfo(r); err != nil {
			fmt.Println("tried to fetch possible collections but got an error:", err)
		} else {
			fmt.Println("possible --collection options:")
			for _, v := range resp {
				fmt.Println("\t", v.GetName())
			}
		}
		os.Exit(1)
	}

	var diffRtt report.Timer
	var diffs report.Meter
	var diff *string
	if rawDiff != nil && len(*rawDiff) > 0 {
		diffServer, diffName := hfileUrlAndName(*rawDiff)
		diff = &diffServer
		diffRtt = r.GetTimer("rtt." + diffName)
		diffs = r.GetMeter("diffs")
		rttName = "rtt." + name
	}

	l := &Load{
		collection: *collection,
		sample:     sample,
		server:     server,
		diff:       diff,
		work:       make(chan bool, (*qps)*(*workers)),
		dropped:    r.GetMeter("dropped"),
		queueSize:  r.GetGuage("queue"),
		rtt:        r.GetTimer(rttName),
		diffRtt:    diffRtt,
		diffs:      diffs,
	}

	if err := l.setKeys(); err != nil {
		fmt.Println("Failed to fetch testing keys:", err)
		os.Exit(1)
	}

	fmt.Printf("Sending %dqps to %s, drawing from %d random keys...\n", *qps, server, len(l.keys))

	l.startWorkers(*workers)
	go l.startKeyFetcher(time.Minute)
	l.generator(*qps)

}
