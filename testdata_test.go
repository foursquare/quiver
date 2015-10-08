package main

import (
	"math/rand"
	"sort"

	"github.com/foursquare/gohfile"
	"github.com/foursquare/quiver/gen"
)

var uncompressed gen.HFileService
var compressed gen.HFileService
var maxKey int

type hasFatal interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

func Setup(t hasFatal) {
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

func GetTestIntReq(name string, keys []int) *gen.SingleHFileKeyRequest {
	keyBytes := make([][]byte, len(keys))
	for i, v := range keys {
		keyBytes[i] = hfile.MockKeyInt(v)
	}
	return &gen.SingleHFileKeyRequest{&name, keyBytes, nil, nil}
}

func MakeTestKeyIntList(r *rand.Rand, count, max int) []int {
	if count < 1 {
		return nil
	}
	keys := make([]int, count)
	for i := 0; i < count; i++ {
		keys[i] = r.Intn(max)
	}
	sort.Ints(keys)
	return keys
}

func GetRandomTestReqs(name string, count, reqSize, max int) []*gen.SingleHFileKeyRequest {
	r := rand.New(rand.NewSource(int64(count + reqSize + max)))

	reqs := make([]*gen.SingleHFileKeyRequest, count)
	for i := 0; i < count; i++ {
		reqs[i] = GetTestIntReq(name, MakeTestKeyIntList(r, reqSize, max))
	}
	return reqs
}
