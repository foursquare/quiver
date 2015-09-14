package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

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

	graphite       string
	graphitePrefix string
}

var Settings SettingDefs

func readSettings() []string {
	s := SettingDefs{}
	flag.IntVar(&s.port, "port", 9999, "listen port")

	flag.BoolVar(&s.debug, "debug", false, "print debug output")

	flag.BoolVar(&s.mlock, "mlock", true, "mlock mapped files in memory (only applies to cmd-line specified files).")

	flag.StringVar(&s.configJsonUrl, "config-json", "", "URL of collection configuration json")

	flag.StringVar(&s.cachePath, "cache", os.TempDir(), "local path to write files fetched (*not* cleaned up automatically)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			`
Usage: %s [options] col1=path1 col2=path2 ...
`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	Settings = s

	if (len(flag.Args()) > 1) == (Settings.configJsonUrl != "") {
		log.Println("Collections must be specified OR URL to configuration json.")
		flag.Usage()
		os.Exit(-1)
	}

	return flag.Args()
}

func getCollectionConfig(args []string) []*hfile.CollectionConfig {
	var configs []*hfile.CollectionConfig
	var err error

	if Settings.configJsonUrl != "" {
		if len(args) > 0 {
			log.Fatalln("Only one of command-line collection specs or json config may be used.")
		}
		configs, err = ConfigsFromJsonUrl(Settings.configJsonUrl)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		configs = make([]*hfile.CollectionConfig, len(args))
		for i, pair := range args {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatal("collections must be specified in the form 'name=path'")
			}
			configs[i] = &hfile.CollectionConfig{parts[0], parts[1], parts[1], Settings.mlock, Settings.debug}
		}
	}

	return configs
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

	name, err := os.Hostname()
	if err != nil {
		name = "localhost"
	}
	log.Printf("Serving on http://%s:%d/ \n", name, Settings.port)

	http.Handle("/rpc/HFileService", NewHttpRpcHandler(cs))
	http.Handle("/", &DebugHandler{cs})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Settings.port), nil))
}
