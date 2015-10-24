// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/foursquare/fsgo/adminz"
	"github.com/foursquare/fsgo/report"
	"github.com/foursquare/quiver/hfile"
)

type SettingDefs struct {
	port  int
	debug bool
	mlock bool

	configJsonUrl string

	cachePath string

	zk            string
	discoveryPath string
}

var Settings SettingDefs

func readSettings() []string {
	s := SettingDefs{}
	flag.IntVar(&s.port, "port", 9999, "listen port")

	flag.BoolVar(&s.debug, "debug", false, "print debug output")

	flag.BoolVar(&s.mlock, "mlock", false, "mlock mapped files in memory rather than copy to heap.")

	flag.StringVar(&s.configJsonUrl, "config-json", "", "URL of collection configuration json")

	flag.StringVar(&s.cachePath, "cache", os.TempDir(), "local path to write files fetched (*not* cleaned up automatically)")

	flag.StringVar(&s.zk, "zookeeper", "", "zookeeper")
	flag.StringVar(&s.discoveryPath, "discovery", "", "service discovery base path")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			`
Usage: %s [options] col1=path1 col2=path2 ...
`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	Settings = s

	if (len(flag.Args()) > 0) == (Settings.configJsonUrl != "") {
		log.Println("Collections must be specified OR URL to configuration json.")
		flag.Usage()
		os.Exit(-1)
	}

	return flag.Args()
}

func main() {
	fmt.Println("max procs:", runtime.GOMAXPROCS(-1))

	graphite := report.Flag()
	args := readSettings()

	stats := report.NewRecorder().
		EnableGCInfoCollection().
		MaybeReportTo(graphite).
		SetAsDefault()

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	registrations := new(Registrations)
	if Settings.discoveryPath != "" {
		registrations.Connect()
		defer registrations.Close()
	}

	configs := getCollectionConfig(args)

	log.Printf("Loading collections (debug %v)...\n", Settings.debug)
	cs, err := hfile.LoadCollections(configs, Settings.cachePath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Serving on http://%s:%d/ \n", hostname, Settings.port)

	if Settings.discoveryPath != "" {
		go registrations.Join(hostname, Settings.discoveryPath, configs, 5*time.Second)
	}

	http.Handle("/rpc/HFileService", NewHttpRpcHandler(cs, stats))
	http.Handle("/", &DebugHandler{cs})

	adminzPages := adminz.New()
	adminzPages.KillfilePaths(adminz.Killfiles(Settings.port))
	adminzPages.Pause(func() error {
		registrations.Leave()
		return nil
	})
	adminzPages.Resume(func() error {
		registrations.Join(hostname, Settings.discoveryPath, configs, 0)
		return nil
	})
	adminzPages.Build()

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Settings.port), nil))
}
