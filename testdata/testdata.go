package testdata

import (
	"math/rand"
	"sort"

	"github.com/dt/quiver/gen"
	"github.com/foursquare/gohfile"
)

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
