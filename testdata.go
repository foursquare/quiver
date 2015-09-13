package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/dt/thile/gen"

	"github.com/foursquare/gohfile"
)

func GetTestIntFile(name string, count int, compress, lock bool) (gen.HFileService, error) {
	path := fmt.Sprintf("testdata/%s.%d.hfile", name, count)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		cmd := fmt.Sprintf("mockhfile -keys %d -compress=%v %s", count, compress, path)
		return nil, fmt.Errorf("%s doesn't exist! generate with:\n\t%s", path, cmd)
	} else if err != nil {
		return nil, err
	}
	cs, err := hfile.LoadCollections([]*hfile.CollectionConfig{{name, path, path, lock, testing.Verbose()}}, os.TempDir())
	return &ThriftRpcImpl{cs}, err
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
