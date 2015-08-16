package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/foursquare/gohfile"
)

type Settings struct {
	port  int
	debug bool
}

func getSettings() Settings {
	s := Settings{}
	flag.IntVar(&s.port, "port", 9999, "listen port")
	flag.BoolVar(&s.debug, "debug", false, "print debug output")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			`
Usage: %s [options] col1@path1 col2@path2 ...

	By default, collections are mmaped and locked into memory.
	Use 'col=path' to serve directly off disk.

	You may need to set 'ulimit -Hl' and 'ulimit -Sl' to allow locking

`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(-1)
	}
	return s
}

func getCollectionConfig(args []string) []hfile.CollectionConfig {
	collections := make([]hfile.CollectionConfig, len(args))
	for i, pair := range flag.Args() {
		parts := strings.SplitN(pair, "=", 2)
		mlock := true
		if len(parts) != 2 {
			mlock = false
			parts = strings.SplitN(pair, "@", 2)
		}
		if len(parts) != 2 {
			log.Fatal("collections must be specified in the form 'name=path' or 'name@path'")
		}
		collections[i] = hfile.CollectionConfig{parts[0], parts[1], mlock}
	}
	return collections
}

func main() {
	s := getSettings()
	configs := getCollectionConfig(flag.Args())

	log.Println("Loading collections...")
	cs, err := hfile.LoadCollections(configs, s.debug)
	if err != nil {
		log.Fatal(err)
	}

	name, err := os.Hostname()
	if err != nil {
		name = "localhost"
	}
	log.Printf("Serving on http://%s:%d/ \n", name, s.port)

	http.Handle("/rpc/HFileService", NewHttpRpcHandler(&s, cs))
	http.Handle("/", &DebugHandler{cs, &s})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil))
}
