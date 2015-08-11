package main

import (
	"log"
	"net/http"
	"sync/atomic"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/dt/thile/gen"
)

type HttpRpcHandler struct {
	reqs int64
	*gen.HFileServiceProcessor
}

func (h *HttpRpcHandler) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	atomic.AddInt64(&h.reqs, 1)
	log.Printf("[#%d] %s %s (%db)", atomic.LoadInt64(&h.reqs), req.Method, req.RequestURI, req.ContentLength)

	in := thrift.NewTMemoryBufferLen(int(req.ContentLength))
	in.ReadFrom(req.Body)
	defer req.Body.Close()

	iprot := thrift.NewTBinaryProtocol(in, true, true)

	outbuf := thrift.NewTMemoryBuffer()
	oprot := thrift.NewTCompactProtocol(outbuf)

	ok, err := h.Process(iprot, oprot)

	if ok {
		outbuf.WriteTo(out)
	} else {
		http.Error(out, err.Error(), 500)
	}
}

func main() {
	impl := new(HfileImpl)
	handler := gen.NewHFileServiceProcessor(impl)
	log.Fatal(http.ListenAndServe(":9000", &HttpRpcHandler{0, handler}))
}
