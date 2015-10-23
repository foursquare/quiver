// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/foursquare/quiver/hfile"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("supply path/to/file as first arg")
		os.Exit(-1)
	}

	if r, err := hfile.NewReader(os.Args[1], os.Args[1], hfile.OnDisk, true); err != nil {
		log.Fatal(err)
	} else {
		r.PrintDebugInfo(os.Stdout, 10)
	}
}
