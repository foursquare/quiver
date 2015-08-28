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

	"github.com/foursquare/gohfile"
)

type SettingDefs struct {
	port  int
	debug bool
	mlock bool

	configJsonUrl string

	cachePath      string
	remotePrefixes RemotePrefix

	graphite       string
	graphitePrefix string
}

var Settings SettingDefs

type RemotePrefix struct {
	prefixes map[string]string
}

func (r *RemotePrefix) String() string {
	return "remote prefix"
}

func (r *RemotePrefix) Set(s string) error {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("remote prefix must be of form /prefix/path=http://example/foo%%s")
	}
	r.prefixes[parts[0]] = parts[1]
	return nil
}

func readSettings() []string {
	s := SettingDefs{}
	s.remotePrefixes.prefixes = make(map[string]string)
	flag.IntVar(&s.port, "port", 9999, "listen port")
	flag.BoolVar(&s.debug, "debug", false, "print debug output")

	flag.BoolVar(&s.mlock, "mem", false, "mlock ALL mapped files to keep them im memory (ignores = vs @ specs).")

	flag.StringVar(&s.configJsonUrl, "config-json", "", "URL of collection configuration json")

	flag.StringVar(&s.cachePath, "cache", os.TempDir(), "local path to write files fetched (*not* cleaned up automatically)")

	flag.StringVar(&s.graphite, "graphite", "", "graphite server to report to")
	flag.StringVar(&s.graphitePrefix, "graphite-prefix", "", "prefix to prepend to reported metrics")

	flag.Var(&s.remotePrefixes, "remote", "/prefix/path=<url-format-string>")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			`
Usage: %s [options] col1=path1 col2=path2 ...

	By default, collections are mmaped and locked into memory.
	Use 'col@path' to serve directly off disk.

	You may need to set 'ulimit -Hl' and 'ulimit -Sl' to allow locking.

`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	Settings = s

	if len(flag.Args()) < 1 && Settings.configJsonUrl == "" {
		log.Println("bah!", Settings)
		flag.Usage()
		os.Exit(-1)
	}

	return flag.Args()
}

func getCollectionConfig(args []string) []hfile.CollectionConfig {
	var configs []hfile.CollectionConfig
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
		configs = make([]hfile.CollectionConfig, len(args))
		for i, pair := range args {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatal("collections must be specified in the form 'name=path' or 'name@path'")
			}
			configs[i] = hfile.CollectionConfig{parts[0], parts[1], parts[1], Settings.mlock, Settings.debug}
		}
	}

	return configs
}

func main() {
	args := readSettings()

	SetupStats(false, Settings.graphite, Settings.graphitePrefix)

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
