package main

import (
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/dt/go-metrics-reporting"
	"github.com/foursquare/gohfile"
	"github.com/foursquare/quiver/gen"
)

type HttpRpcHandler struct {
	stats *report.Recorder
	*gen.HFileServiceProcessor
}

func NewHttpRpcHandler(cs *hfile.CollectionSet, stats *report.Recorder) *HttpRpcHandler {
	impl := gen.NewHFileServiceProcessor(&ThriftRpcImpl{cs})
	return &HttpRpcHandler{stats, impl}
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
		if p.stats != nil {
			p.stats.TimeSince(name, start)
		}
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
		var in *thrift.TMemoryBuffer
		size := int(req.ContentLength)
		if size > 0 {
			in = thrift.NewTMemoryBufferLen(size)
		} else {
			in = thrift.NewTMemoryBuffer()
		}

		in.ReadFrom(req.Body)
		defer req.Body.Close()

		compact := false

		if in.Len() > 0 && in.Bytes()[0] == thrift.COMPACT_PROTOCOL_ID {
			compact = true
		}

		outbuf := thrift.NewTMemoryBuffer()

		var iprot thrift.TProtocol
		var oprot thrift.TProtocol

		if compact {
			iprot = thrift.NewTCompactProtocol(in)
			oprot = thrift.NewTCompactProtocol(outbuf)
		} else {
			iprot = thrift.NewTBinaryProtocol(in, true, true)
			oprot = thrift.NewTBinaryProtocol(outbuf, true, true)
		}

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
