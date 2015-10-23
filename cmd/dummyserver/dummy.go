// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/foursquare/go-metrics"
)

var timer metrics.Timer

func helloworld(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func simpleGetValuesSingle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s1 := "800100020000000f67657456616c75657353696e676c65"
	b, _ := ioutil.ReadAll(r.Body)
	n := hex.EncodeToString(b[23:27])
	s2 := "0c00000d0001080b00000000080002000000000000"

	s := s1 + n + s2
	data := make([]byte, len(s)/2)
	hex.Decode(data, []byte(s))
	w.Write(data)
	timer.UpdateSince(start)
}

func main() {
	r := metrics.NewRegistry()
	timer = metrics.GetOrRegisterTimer("rtt", r)
	go metrics.LogScaled(r, time.Second*5, time.Millisecond, log.New(os.Stderr, "\t", 0))

	http.HandleFunc("/rpc/HFileService", simpleGetValuesSingle)
	http.HandleFunc("/", helloworld)
	http.ListenAndServe(":9999", nil)
}
