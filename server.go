package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/dt/thile/gen"
)

type Settings struct {
	listen int
}

func getSettings() Settings {
	s := Settings{}
	flag.IntVar(&s.listen, "port", 9999, "listen port")

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

func getCollectionConfig(args []string) []Collection {
	collections := make([]Collection, len(args))
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
		collections[i] = Collection{parts[0], parts[1], mlock, nil}
	}
	return collections
}

func main() {
	s := getSettings()
	collections := getCollectionConfig(flag.Args())

	log.Println("Loading collections...")
	cs, err := LoadCollections(collections)
	if err != nil {
		log.Fatal(err)
	}

	name, err := os.Hostname()
	if err != nil {
		name = "localhost"
	}
	log.Printf("Serving on http://%s:%d/ \n", name, s.listen)

	impl := gen.NewHFileServiceProcessor(cs)
	http.Handle("/rpc/HfileService", &HttpRpcHandler{impl})
	http.Handle("/", &DebugHandler{cs})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.listen), nil))
}
