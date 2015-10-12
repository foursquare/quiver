package main

import (
	"github.com/dt/go-metrics-reporting"
	"github.com/dt/httpthrift"
	"github.com/foursquare/quiver/hfile"
	"github.com/foursquare/quiver/gen"
)

func NewHttpRpcHandler(cs *hfile.CollectionSet, stats *report.Recorder) *httpthrift.ThriftOverHTTPHandler {
	return httpthrift.NewThriftOverHTTPHandler(gen.NewHFileServiceProcessor(&ThriftRpcImpl{cs}), stats)
}
