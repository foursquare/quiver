package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/dt/go-metrics-reporting"
	"github.com/foursquare/gohfile"
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

	flag.BoolVar(&s.mlock, "mlock", true, "mlock mapped files in memory (only applies to cmd-line specified files).")

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
	graphite := report.Flag()
	args := readSettings()

	report.NewRecorder().
		EnableGCInfoCollection().
		MaybeReportTo(graphite).
		SetAsDefault()

	configs := getCollectionConfig(args)

	log.Printf("Loading collections (debug %v)...\n", Settings.debug)
	cs, err := hfile.LoadCollections(configs, Settings.cachePath)
	if err != nil {
		log.Fatal(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	registrations := new(Registrations)

	if Settings.discoveryPath != "" {
		registrations.Connect()
		registrations.Join(hostname, Settings.discoveryPath, configs)
		defer registrations.Close()
	}

	log.Printf("Serving on http://%s:%d/ \n", hostname, Settings.port)

	http.Handle("/rpc/HFileService", NewHttpRpcHandler(cs))
	http.Handle("/", &DebugHandler{cs})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Settings.port), nil))
}
