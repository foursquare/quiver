// Copyright (C) 2017 Foursquare Labs Inc.
// +build !native_snappy

package hfile

import "github.com/cockroachdb/c-snappy"

func snappyDecode(target []byte, data []byte) ([]byte, error) {
	return snappy.Decode(target, data)
}
