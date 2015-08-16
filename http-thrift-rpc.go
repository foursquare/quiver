package main

import (
	"net/http"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/dt/thile/gen"
	"github.com/foursquare/gohfile"
)

type HttpRpcHandler struct {
	*gen.HFileServiceProcessor
}

func NewHttpRpcHandler(settings *Settings, cs *hfile.CollectionSet) *HttpRpcHandler {
	impl := gen.NewHFileServiceProcessor(&ThriftRpcImpl{cs, settings})
	return &HttpRpcHandler{impl}
}

func (h *HttpRpcHandler) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		in := thrift.NewTMemoryBufferLen(int(req.ContentLength))
		in.ReadFrom(req.Body)
		defer req.Body.Close()

		iprot := thrift.NewTBinaryProtocol(in, true, true)

		outbuf := thrift.NewTMemoryBuffer()
		oprot := thrift.NewTBinaryProtocol(outbuf, true, true)

		ok, err := h.Process(iprot, oprot)

		if ok {
			outbuf.WriteTo(out)
		} else {
			http.Error(out, err.Error(), 500)
		}
	} else {
		http.Error(out, "Must POST TBinary encoded thrift RPC", 401)
	}
}
