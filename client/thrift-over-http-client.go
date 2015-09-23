package thttp

import (
	"io"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/foursquare/quiver/gen"
)

type ThriftOverHttpClient struct {
	transport *http.Client
	url       string
	sendbuf   *thrift.TMemoryBuffer
	client    *gen.HFileServiceClient
}

type ThriftOverHttpSendProt struct {
	owner *ThriftOverHttpClient
	*thrift.TBinaryProtocol
}

func (t *ThriftOverHttpSendProt) Flush() error {
	req, err := http.NewRequest("POST", t.owner.url, t.owner.sendbuf)
	req.Header.Set("Content-Length", string(t.owner.sendbuf.Len()))
	req.Header.Set("Content-Type", "application/x-thrift")

	resp, err := t.owner.transport.Do(req)
	if err != nil {
		return err
	}

	size := int(resp.ContentLength)
	var respbuf *thrift.TMemoryBuffer
	if size > 0 {
		respbuf = thrift.NewTMemoryBufferLen(size)
	} else {
		respbuf = thrift.NewTMemoryBuffer()
	}

	io.Copy(respbuf, resp.Body)
	resp.Body.Close()
	t.owner.client.InputProtocol = thrift.NewTBinaryProtocol(respbuf, true, true)

	return nil
}

func NewThriftHttpRpcClient(url string) *gen.HFileServiceClient {
	n := &http.Client{}
	t := &ThriftOverHttpClient{n, url, thrift.NewTMemoryBuffer(), nil}
	send := &ThriftOverHttpSendProt{t, thrift.NewTBinaryProtocol(t.sendbuf, true, true)}
	c := gen.NewHFileServiceClientProtocol(nil, nil, send)
	t.client = c
	return c
}
