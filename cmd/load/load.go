package main

import (
	"flag"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/dt/go-metrics"
	"github.com/dt/thile/client"
	"github.com/dt/thile/gen"
	"github.com/foursquare/gohfile"
)

func RandomReq(name string, min, max int) *gen.SingleHFileKeyRequest {
	count := 10
	if false {
		count = int(math.Abs(rand.NormFloat64())*3 + 3)
	}
	keys := make([]int, count)
	lim := min + max

	for i := 0; i < count; i++ {
		k := rand.Intn(lim) + min
		keys[i] = k
	}
	sort.Ints(keys)

	keyBytes := make([][]byte, len(keys))
	for i, v := range keys {
		keyBytes[i] = hfile.MockKeyInt(v)
	}
	return &gen.SingleHFileKeyRequest{&name, keyBytes, nil, nil}
}

func Send(client *gen.HFileServiceClient, r *gen.SingleHFileKeyRequest) {
	var err error
	_, err = client.GetValuesSingle(r)

	if err != nil {
		log.Println(err)
	}
}

func generator(work chan bool, qps int, d metrics.Meter, q metrics.Gauge) {
	second := time.Second.Nanoseconds()
	pause := time.Nanosecond * time.Duration(second/int64(qps))

	for _ = range time.Tick(pause) {
		q.Update(int64(len(work)))
		select {
		case work <- true:
		default:
			d.Mark(1)
		}
	}
}

func SendOne(url, name string, min, max int, m metrics.Timer) {
	start := time.Now()
	r := RandomReq(name, min, max)
	c := thttp.NewThriftHttpRpcClient(url)
	if true {
		Send(c, r)
	} else {
		time.Sleep(2 * time.Millisecond)
	}
	m.UpdateSince(start)
}

func worker(work chan bool, url, name string, min, max int, m metrics.Timer) {
	for true {
		<-work
		SendOne(url, name, min, max, m)
	}
}

func main() {
	url := flag.String("url", "http://localhost:9999/rpc/HFileService", "URL of hfile server")
	name := flag.String("name", "testdata", "name of collection")
	min := flag.Int("min", 0, "min int key")
	max := flag.Int("max", -1, "max int key (required)")
	workers := flag.Int("workers", 8, "worker pool size")

	qps := flag.Int("qps", 100, "qps to attempt")

	flag.Parse()

	if *max < 0 {
		log.Fatal("max must be specified and >0")
	}

	w := make(chan bool, (*qps)*(*workers)*3)
	r := metrics.NewRegistry()
	m := metrics.GetOrRegisterTimer("rtt", r)
	d := metrics.GetOrRegisterMeter("dropped", r)
	q := metrics.GetOrRegisterGauge("queue", r)

	go metrics.LogScaled(r, time.Second*5, time.Millisecond, log.New(os.Stderr, "\t", 0))

	for i := 0; i < *workers; i++ {
		go worker(w, *url, *name, *min, *max, m)
	}

	generator(w, *qps, d, q)

}
