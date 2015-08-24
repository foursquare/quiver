package main

import (
	"net/http"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/dt/thile/gen"
	"github.com/foursquare/gohfile"
	"github.com/rcrowley/go-metrics"
)

type HttpRpcHandler struct {
	*gen.HFileServiceProcessor
}

func NewHttpRpcHandler(cs *hfile.CollectionSet) *HttpRpcHandler {
	impl := gen.NewHFileServiceProcessor(&ThriftRpcImpl{cs})
	return &HttpRpcHandler{impl}
}

// borrowed from generated thrift code, but with instrumentation added.
func (p *HttpRpcHandler) Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}

	if processor, ok := p.GetProcessorFunction(name); ok {
		start := time.Now()
		success, err = processor.Process(seqId, iprot, oprot)
		end := time.Now()
		metrics.GetOrRegisterTimer(name, Stats).Update(end.Sub(start))
		return
	}

	iprot.Skip(thrift.STRUCT)
	iprot.ReadMessageEnd()
	e := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)

	oprot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
	e.Write(oprot)
	oprot.WriteMessageEnd()
	oprot.Flush()

	return false, e
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
