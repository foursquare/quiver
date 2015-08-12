package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/dt/thile/gen"
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
		oprot := thrift.NewTCompactProtocol(outbuf)

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
				if len(parts) > 1 {
					key := make([]byte, len(parts[1])/2)
					n, err := hex.Decode(key, []byte(parts[1]))
					if err != nil {
						http.Error(out, err.Error(), 401)
					} else {
						values := reader.GetAll(key)
						if len(values) > 0 {
							for _, value := range values {
								fmt.Fprintf(out, "%s %v\n", value, value)
							}
						} else {
							http.Error(out, fmt.Sprintf("Not found: %s/%v (%db)", parts[1], key, n), 404)
						}
					}
				} else {
					fmt.Fprintln(out, "entries: ", reader.header.entryCount)
					fmt.Fprintln(out, "blocks: ", len(reader.dataIndex.dataBlocks))
					for i, blk := range reader.dataIndex.dataBlocks {
						fmt.Fprintf(out, "\t#%d: %s (%v)\n", i, blk.firstKeyBytes, blk.firstKeyBytes)
					}
				}
			}
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s coll1=path1 coll2=path2 ...", os.Args[0])
	}
	collections := make([]Collection, len(os.Args)-1)
	for i, pair := range os.Args[1:] {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			log.Fatal("collections must be specified in the form'name=path'")
		}
		collections[i] = Collection{parts[0], parts[1]}
	}
	cs, err := LoadCollections(collections)
	if err != nil {
		log.Fatal(err)
	}
	handler := gen.NewHFileServiceProcessor(cs)
	log.Fatal(http.ListenAndServe(":9000", &HttpRpcHandler{0, cs, handler}))
}
