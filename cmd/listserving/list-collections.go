// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/foursquare/fsgo/net/thriftrpc"
	"github.com/foursquare/quiver/gen"
)

func removeWebhdfs(s string) string {
	from := "webhdfs/v1"
	if p := strings.Index(s, from); p > 0 {
		return "webhdfs: " + strings.Split(s[p+len(from):], "?")[0]
	}
	return s
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("usage: %s hfileserver\n", os.Args[0])
		os.Exit(1)
	}

	url := os.Args[1]

	if !strings.Contains(url, "/") {
		url = url + "/rpc/HFileService"
	}
	if !strings.HasPrefix(url, "http") {
		url = "http://" + url
	}

	recv, send := thriftrpc.NewClientProts(url, false)
	client := gen.NewHFileServiceClientProtocol(nil, recv, send)

	r := &gen.InfoRequest{nil, nil}
	if resp, err := client.GetInfo(r); err != nil {
		fmt.Println("Error getting info:", err)
		os.Exit(1)
	} else {
		for _, v := range resp {
			fmt.Printf("%s:\n", v.GetName())
			fmt.Printf("\tpath:\t%s\n", removeWebhdfs(v.GetPath()))
			fmt.Printf("\tkeys:\t%d\n", v.GetNumElements())
			fmt.Printf("\tstart:\t%s\n", hex.EncodeToString(v.GetFirstKey()))
			fmt.Printf("\tend:\t%s\n", hex.EncodeToString(v.GetLastKey()))
		}
	}
}
