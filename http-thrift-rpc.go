// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"github.com/foursquare/fsgo/net/httpthrift"
	"github.com/foursquare/fsgo/report"
	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/hfile"
)

func NewHttpRpcHandler(cs *hfile.CollectionSet, stats *report.Recorder) *httpthrift.ThriftOverHTTPHandler {
	return httpthrift.NewThriftOverHTTPHandler(gen.NewHFileServiceProcessor(&ThriftRpcImpl{cs}), stats)
}
