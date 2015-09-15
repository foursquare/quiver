package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/dt/go-curator-discovery"
	"github.com/dt/go-metrics-reporting"
	"github.com/flier/curator.go"
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

func getCollectionConfig(args []string) ([]*hfile.CollectionConfig, []Registration) {
	var configs []*hfile.CollectionConfig
	var reg []Registration
	var err error

	if Settings.configJsonUrl != "" {
		if len(args) > 0 {
			log.Fatalln("Only one of command-line collection specs or json config may be used.")
		}
		configs, reg, err = ConfigsFromJsonUrl(Settings.configJsonUrl)
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

	return configs, reg
}

func main() {
	graphite := report.Flag()
	args := readSettings()

	report.NewRecorder().
		EnableGCInfoCollection().
		MaybeReportTo(graphite).
		SetAsDefault()

	configs, reg := getCollectionConfig(args)

	log.Printf("Loading collections (debug %v)...\n", Settings.debug)
	cs, err := hfile.LoadCollections(configs, Settings.cachePath)
	if err != nil {
		log.Fatal(err)
	}

	name, err := os.Hostname()
	if err != nil {
		name = "localhost"
	}

	if Settings.discoveryPath != "" {
		if len(reg) < 1 {
			log.Fatal("no registrations! (service reg only supported when configured via json)")
		}
		if Settings.zk == "" {
			log.Fatal("Specified discovery path but not zk?", Settings.zk)
		}
		retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)
		zk := curator.NewClient(Settings.zk, retryPolicy)
		if err := zk.Start(); err != nil {
			log.Fatal(err)
		}
		defer zk.Close()

		for _, i := range reg {
			disco := discovery.NewServiceDiscovery(zk, curator.JoinPath(Settings.discoveryPath, i.base))
			if err := disco.MaintainRegistrations(); err != nil {
				log.Fatal(err)
			}
			s := discovery.NewSimpleServiceInstance(i.name, name, Settings.port)
			disco.Register(s)
		}
	}

	log.Printf("Serving on http://%s:%d/ \n", name, Settings.port)

	http.Handle("/rpc/HFileService", NewHttpRpcHandler(cs))
	http.Handle("/", &DebugHandler{cs})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Settings.port), nil))
}
