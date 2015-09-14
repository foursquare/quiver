package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"testing"

	"github.com/dt/thile/gen"
	"github.com/dt/thile/testdata"
	"github.com/foursquare/gohfile"
)

var uncompressed gen.HFileService
var compressed gen.HFileService
var maxKey int

func TestMain(m *testing.M) {
	maxKey = 15000000
	if cs, err := testdata.GetTestIntFile("uncompressed", maxKey, false, true); err != nil {
		log.Fatal(err)
	} else {
		uncompressed = &ThriftRpcImpl{cs}
	}
	if cs, err := testdata.GetTestIntFile("compressed", maxKey, true, true); err != nil {
		log.Fatal(err)
	} else {
		compressed = &ThriftRpcImpl{cs}
	}

	os.Exit(m.Run())
}

func TestGetValuesSingle(t *testing.T) {
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
	reqs := testdata.GetRandomTestReqs("compressed", b.N, 5, maxKey)
	b.StartTimer()

	for _, req := range reqs {
		if _, err := compressed.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		}
	}
}
