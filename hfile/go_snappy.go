// Copyright (C) 2017 Foursquare Labs Inc.
// +build go_snappy
// note: this is an experiment to drop-in replace c-snappy with golang/snappy.
//   It buys us simpler builds at the cost of unknown performance impact (needs
//   testing). To activate it, build with `go build -tags go_snappy`.

package hfile

import "github.com/golang/snappy"

func snappyDecode(target []byte, data []byte) ([]byte, error) {
	return snappy.Decode(target, data)
}
