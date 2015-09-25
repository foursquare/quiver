package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/foursquare/gohfile"
	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/testdata"
)

var uncompressed gen.HFileService
var compressed gen.HFileService
var maxKey int

func Setup(t interface {
	Fatal(args ...interface{})
}) {
	maxKey = 5000000
	if uncompressed != nil && compressed != nil {
		return
	}
	if cs, err := hfile.TestdataCollectionSet("uncompressed", maxKey, false, true); err != nil {
		t.Fatal(err)
	} else {
		uncompressed = &ThriftRpcImpl{cs}
	}
	if cs, err := hfile.TestdataCollectionSet("compressed", maxKey, true, true); err != nil {
		t.Fatal(err)
	} else {
		compressed = &ThriftRpcImpl{cs}
	}
}

func TestGetValuesSingle(t *testing.T) {
	Setup(t)
	reqs := testdata.GetRandomTestReqs("compressed", 10, 5, maxKey)

	for _, req := range reqs {
		if r, err := compressed.GetValuesSingle(req); err != nil {
			t.Fatal("error: ", err)
		} else {
			if len(r.GetValues()) != len(req.GetSortedKeys()) {
				t.Fatal("wrong number of results: ", "\n", req.GetSortedKeys(), "\n", r.GetValues())
			}
			for i, k := range req.SortedKeys {
				key := binary.BigEndian.Uint32(k)
				expected := hfile.MockValueInt(int(key))
				actual := r.GetValues()[int32(i)]
				if !bytes.Equal(actual, expected) {
					t.Fatalf("mismatched value for key %d (%d): found '%v' expected '%v'", i, key, actual, expected)
				}
			}
		}
	}

	dupes := []int{1, 2, 3, 3, 3, 4}
	req := testdata.GetTestIntReq("compressed", dupes)
	if r, err := compressed.GetValuesSingle(req); err != nil {
		t.Fatal("error: ", err)
	} else {
		if len(r.Values) != len(dupes) {
			t.Fatal("wrong number of results... ignored dupes?", len(r.Values), len(dupes))
		}
		if r.GetKeyCount() != int32(len(dupes)) {
			t.Fatal("wrong key count... ignored dupes?", r.GetKeyCount(), len(dupes))
		}
	}

}

func BenchmarkUncompressed(b *testing.B) {
	b.StopTimer()
	Setup(b)
	reqs := testdata.GetRandomTestReqs("uncompressed", b.N, 5, maxKey)
	b.StartTimer()

	for _, req := range reqs {
		if _, err := uncompressed.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		}
	}
}

func BenchmarkCompressed(b *testing.B) {
	b.StopTimer()
	Setup(b)
	reqs := testdata.GetRandomTestReqs("compressed", b.N, 5, maxKey)
	b.StartTimer()

	for _, req := range reqs {
		if _, err := compressed.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		}
	}
}
