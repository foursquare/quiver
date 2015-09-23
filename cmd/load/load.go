package main

import (
	"bufio"
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

	"github.com/dt/go-metrics"
	"github.com/dt/go-metrics-reporting"
	"github.com/foursquare/quiver/client"
	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/util"
)

type Load struct {
	collection string
	sample     *int64
	keys       [][]byte

	server string
	diff   *string // optional

	work chan bool

	rtt     string
	diffRtt string

	queueSize report.Guage
	dropped   report.Meter

	// for atomic keyset swaps in setKeys.
	sync.RWMutex
}

// Pick a random request type to generate and send.
func (l *Load) sendOne(client *gen.HFileServiceClient, diff *gen.HFileServiceClient) {
	i := rand.Int31n(100)
	switch {
	case i < 15:
		l.sendGetIterator(client, diff)
	// case i < 30:
	// 	l.sendPrefixes(client, diff)
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
		//log.Println("Fetching new keys...")
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

func PrintSummary(rttName, diffRtt string, isDiffing bool) {
	du := float64(time.Millisecond)
	overall := report.GetDefault().Get(rttName + ".overall").(metrics.Timer)

	if isDiffing {
		diffOverall := report.GetDefault().Get(diffRtt + ".overall").(metrics.Timer)
		useful := []float64{0.50, 0.90, 0.99}
		ps, psDiff := overall.Percentiles(useful), diffOverall.Percentiles(useful)
		fmt.Printf("%10s\t\t%10s\n", rttName, diffRtt)
		fmt.Printf("\tp99 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[2]/du, psDiff[2]/du, (ps[2]-psDiff[2])/du)
		fmt.Printf("\tp90 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[1]/du, psDiff[1]/du, (ps[1]-psDiff[1])/du)
		fmt.Printf("\tp50 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[0]/du, psDiff[0]/du, (ps[0]-psDiff[0])/du)
		report.GetDefault().Each(func(stat string, i interface{}) {
			if strings.HasPrefix(stat, "diffs.") {
				switch m := i.(type) {
				case metrics.Meter:
					fmt.Printf("%s %d (%d total)\n", m.Rate1(), m.Count())
				default:
					fmt.Printf("%s %T %v\n", m, m)
				}
			}
		})
	} else {
		fmt.Printf("%s\t(%6.2fqps)\tp99 %6.2fms\n", rttName, overall.Rate1(), overall.Percentile(0.99)/du)
	}
	queue := report.GetDefault().Get("queue").(metrics.Gauge).Value()
	dropped := report.GetDefault().Get("dropped").(metrics.Meter)
	fmt.Printf("queue %d (dropped: %.2f)\n", queue, dropped.Rate1())
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

	diffRtt := ""
	diffName := ""
	var diff *string

	if rawDiff != nil && len(*rawDiff) > 0 {
		diffServer, diffName := hfileUrlAndName(*rawDiff)
		diff = &diffServer
		diffRtt = "rtt." + diffName
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
		rtt:        rttName,
		diffRtt:    diffRtt,
	}

	if err := l.setKeys(); err != nil {
		fmt.Println("Failed to fetch testing keys:", err)
		os.Exit(1)
	}
	fmt.Printf("Sending %dqps to %s (%s), drawing from %d random keys...\n", *qps, name, server, len(l.keys))
	if l.diff != nil {
		fmt.Printf("Diffing against %s (%s)\n", diffName, *l.diff)
	}

	l.startWorkers(*workers)
	go l.startKeyFetcher(time.Minute)
	go l.generator(*qps)

	fmt.Print("Press enter for stats summary.\n")
	reader := bufio.NewReader(os.Stdin)

	for {
		reader.ReadString('\n')
		PrintSummary(rttName, diffRtt, l.diff != nil)
	}
}
