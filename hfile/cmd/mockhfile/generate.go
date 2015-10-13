package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/foursquare/quiver/hfile"
)

func main() {
	compress := flag.Bool("compress", false, "compression")
	blockSize := flag.Int("blocksize", 4098, "block size in bytes")
	verbose := flag.Bool("verbose", false, "verbose output")

	keys := flag.Int("keys", 10000, "number of k-v pairs to generate")

	flag.Parse()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nUsage: %s [options] path/to/file\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(-1)
	}

	if err := hfile.GenerateMockHfile(flag.Arg(0), *keys, *blockSize, *compress, *verbose, true); err != nil {
		log.Fatal(err)
	}
}
