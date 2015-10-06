package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dt/go-curator-discovery"
	"github.com/dt/go-metrics-reporting"
	"github.com/dt/httpthrift"
	"github.com/foursquare/quiver/gen"
)

type Load struct {
	collection string
	sample     *int64
	keys       [][]byte

	server func() string

	diffing bool
	diff    func() string

	work chan bool

	rtt     string
	diffRtt string

	queueSize report.Guage
	dropped   report.Meter

	mixPrefix, mixIterator, mixMulti int32

	// for atomic keyset swaps in setKeys.
	sync.RWMutex
}

func GetQuiverClient(url func() string) *gen.HFileServiceClient {
	recv, send := httpthrift.NewDynamicClientProts(url)
	return gen.NewHFileServiceClientProtocol(nil, recv, send)
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

// given a string like testing=fsan44:20202, return (http://fsan44:20202/rpc/HFileService, testing).
func hfileUrlAndName(s string) (func() string, string, discovery.Conn) {
	name := strings.NewReplacer("http://", "", ".", "_", ":", "_", "/", "_").Replace(s)

	if parts := strings.Split(s, "="); len(parts) > 1 {
		s = parts[1]
		name = parts[0]
	}

	if strings.HasPrefix(s, "zk:") {
		if len(zk) < 1 {
			log.Fatal("must specify --zk to use discovery")
		}
		s := s[len("zk:"):]
		shardAndPath := strings.Split(s, "@")
		if len(shardAndPath) != 2 {
			log.Fatal("format: zk:$SHARD@$PATH")
		}
		shard, path := shardAndPath[0], shardAndPath[1]

		disco, conn, err := discovery.NewServiceDiscoveryAndConn(zk, path)
		if err != nil {
			log.Fatal(err)
		} else {
			disco.Watch()
		}

		log.Printf("discovering instances of %s at %s\n", shard, path)
		provider := disco.Provider(shard)
		f := func() string {
			i, err := provider.GetInstance()
			if err != nil {
				log.Println("error discovering instance:", err)
			} else if i == nil {
				log.Println("no instances found")
			} else {
				return fmt.Sprintf("http://%s/rpc/HFileService", i.Spec())
			}
			return ""
		}

		return f, name, conn
	}

	if !strings.Contains(s, "/") {
		fmt.Printf("'%s' doens't appear to specify a path. Appending /rpc/HFileService...\n", s)
		s = s + "/rpc/HFileService"
	}

	if !strings.HasPrefix(s, "http") {
		s = "http://" + s
	}
	return func() string { return s }, name, nil
}

var zk = ""

func main() {
	orig := flag.String("server", "localhost:9999", "URL of hfile server")
	rawDiff := flag.String("diff", "", "URL of second hfile server to compare")
	collection := flag.String("collection", "", "name of collection")
	graphite := report.Flag()
	workers := flag.Int("workers", 8, "worker pool size")
	flag.StringVar(&zk, "zk", "", "zookeeper host")
	qps := flag.Int("qps", 100, "qps to attempt")
	sample := flag.Int64("sampleSize", 1000, "number of random keys to use")

	mixPrefix := flag.Int("mix-prefix", 10, "getPrefixes traffic mix % (un-alloc is getSingle)")
	mixIter := flag.Int("mix-iterator", 10, "getPrefixes traffic mix % (un-alloc is getSingle)")
	mixMulti := flag.Int("mix-multi", 20, "getPrefixes traffic mix % (un-alloc is getSingle)")

	flag.Parse()

	r := report.NewRecorder().
		MaybeReportTo(graphite).
		SetAsDefault()

	rttName := "rtt"
	server, name, conn := hfileUrlAndName(*orig)
	if conn != nil {
		defer conn.Close()
	}

	if collection == nil || len(*collection) < 1 {
		fmt.Println("--collection is required")
		c := GetQuiverClient(server)
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

	diffing := false
	diffRtt := ""
	diffName := ""
	diff := func() string { return "" }

	if rawDiff != nil && len(*rawDiff) > 0 {
		diffing = true
		diff, diffName, conn = hfileUrlAndName(*rawDiff)
		if conn != nil {
			defer conn.Close()
		}
		diffRtt = "rtt." + diffName
		rttName = "rtt." + name
	}

	l := &Load{
		collection:  *collection,
		sample:      sample,
		server:      server,
		diffing:     diffing,
		diff:        diff,
		work:        make(chan bool, (*qps)*(*workers)),
		dropped:     r.GetMeter("dropped"),
		queueSize:   r.GetGuage("queue"),
		rtt:         rttName,
		diffRtt:     diffRtt,
		mixPrefix:   int32(*mixPrefix),
		mixIterator: int32(*mixPrefix + *mixIter),
		mixMulti:    int32(*mixPrefix + *mixIter + *mixMulti),
	}

	if err := l.setKeys(); err != nil {
		fmt.Println("Failed to fetch testing keys:", err)
		os.Exit(1)
	}
	fmt.Printf("Sending %dqps to %s (%s), drawing from %d random keys...\n", *qps, name, server(), len(l.keys))
	if l.diffing {
		fmt.Printf("Diffing against %s (%s)\n", diffName, l.diff())
	}

	l.startWorkers(*workers)
	go l.startKeyFetcher(time.Minute)
	go l.generator(*qps)

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Press enter for stats summary.\n")
		reader.ReadString('\n')
		l.PrintSummary()
	}
}
