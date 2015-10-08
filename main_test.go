package main

import (
	"net/http/httptest"
	"testing"

	"github.com/dt/httpthrift"
	"github.com/foursquare/quiver/gen"
)

func DummyServer(t hasFatal) *httptest.Server {
	Setup(t)
	return httptest.NewServer(NewHttpRpcHandler(compressed.(*ThriftRpcImpl).CollectionSet, nil))
}

func DummyClient(url string, compact bool) *gen.HFileServiceClient {
	recv, send := httpthrift.NewClientProts(url, compact)
	return gen.NewHFileServiceClientProtocol(nil, recv, send)
}

func TestRoundTrip(t *testing.T) {
	Setup(t)
	srv := DummyServer(t)
	defer srv.Close()
	client := DummyClient(srv.URL, false)
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
	srv := DummyServer(b)
	defer srv.Close()
	client := DummyClient(srv.URL, false)
	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)
	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			b.StopTimer()
			CheckReqAndRes(b, req, res)
			b.StartTimer()
		}
	}
}

// The same as above except we flip the `compact` flag in the client.
func TestRoundTripTCompact(t *testing.T) {
	Setup(t)
	srv := DummyServer(t)
	defer srv.Close()
	client := DummyClient(srv.URL, true)
	reqs := GetRandomTestReqs("compressed", 100, 5, 50000)

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			t.Fatal("error: ", err)
		} else {
			CheckReqAndRes(t, req, res)
		}
	}
}

// The same as above except we flip the `compact` flag in the client.
func BenchmarkTCompact(b *testing.B) {
	b.StopTimer()
	srv := DummyServer(b)
	defer srv.Close()
	client := DummyClient(srv.URL, true)
	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)
	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			b.StopTimer()
			CheckReqAndRes(b, req, res)
			b.StartTimer()
		}
	}
}
