package main

import (
	"net/http/httptest"
	"testing"

	"github.com/dt/httpthrift"
	"github.com/foursquare/quiver/gen"
)

func DummyClient(t hasFatal, size int) (*gen.HFileServiceClient, *httptest.Server) {
	ts := httptest.NewServer(NewHttpRpcHandler(compressed.(*ThriftRpcImpl).CollectionSet, nil))
	recv, send := httpthrift.NewClientProts(ts.URL)
	return gen.NewHFileServiceClientProtocol(nil, recv, send), ts
}

func TestRoundTrip(t *testing.T) {
	Setup(t)
	client, srv := DummyClient(t, 50000)
	defer srv.Close()
	reqs := GetRandomTestReqs("compressed", 100, 5, 50000)

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			t.Fatal("error: ", err)
		} else {
			CheckReqAndRes(t, req, res)
		}
	}
}

func BenchmarkServer(b *testing.B) {
	b.StopTimer()
	Setup(b)
	client, srv := DummyClient(b, 50000)
	defer srv.Close()
	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)
	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			CheckReqAndRes(b, req, res)
		}
	}
}
