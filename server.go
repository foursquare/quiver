package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/dt/thile/gen"
	"github.com/paperstreet/gohfile/hfile"
)

type HttpRpcHandler struct {
	reqs int64
	*CollectionSet
	*gen.HFileServiceProcessor
}

func (h *HttpRpcHandler) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	atomic.AddInt64(&h.reqs, 1)
	log.Printf("[#%d] %s %s (%db)", atomic.LoadInt64(&h.reqs), req.Method, req.RequestURI, req.ContentLength)

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
		parts := strings.Split(req.RequestURI[1:], "/")
		if len(parts) < 1 {
			http.Error(out, "GET requests must specify /coll/key", 401)
		} else {
			col := parts[0]
			reader, err := h.readerFor(col)
			if err != nil {
				http.Error(out, err.Error(), 500)
			} else {
				scanner := hfile.NewScanner(reader)
				if len(parts) > 1 {
					key := make([]byte, len(parts[1])/2)
					n, err := hex.Decode(key, []byte(parts[1]))
					if err != nil {
						http.Error(out, err.Error(), 401)
					} else {
						log.Print("[Debug] key: %v", key)
						values, err := scanner.GetAll(key)
						if err != nil {
							http.Error(out, err.Error(), 500)
						}
						if len(values) > 0 {
							for _, value := range values {
								fmt.Fprintf(out, "%s %v\n", value, value)
							}
						} else {
							http.Error(out, fmt.Sprintf("Not found: %s/%v (%db)", parts[1], key, n), 404)
						}
					}
				} else {
					reader.PrintDebugInfo(out)
				}
			}
		}
	}
}

type Settings struct {
	listen int
	mlock  bool
}

func main() {
	s := Settings{}
	flag.IntVar(&s.listen, "port", 9999, "listen port")
	flag.BoolVar(&s.mlock, "mlock", false, "mlock mapped hfiles")
	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Fatalf("usage: %s coll1=path1 coll2=path2 ...", os.Args[0])
	}

	collections := make([]Collection, len(flag.Args()))
	for i, pair := range flag.Args() {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			log.Fatal("collections must be specified in the form'name=path'")
		}
		collections[i] = Collection{parts[0], parts[1]}
	}
	cs, err := LoadCollections(collections, s.mlock)
	if err != nil {
		log.Fatal(err)
	}
	handler := gen.NewHFileServiceProcessor(cs)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.listen), &HttpRpcHandler{0, cs, handler}))
}
