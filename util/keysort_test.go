package util

import (
	"bytes"
	"sort"
	"testing"
)

func TestKeysort(t *testing.T) {
	abc := []byte("abc")
	ab := []byte("ab")
	bbc := []byte("bbc")
	d := []byte("d")
	e := []byte("e")
	ff := []byte("ff")

	keys := [][]byte{ff, bbc, ab, abc, e, d}
	correct := [][]byte{ab, abc, bbc, d, e, ff}
	// t.Log("\n" + PrettyKeys(keys))

	sort.Sort(Keys(keys))

	// t.Log("\n" + PrettyKeys(keys))
	for i, _ := range correct {
		if !bytes.Equal(correct[i], keys[i]) {
			t.Fatal("unexpected byte", i, correct[i], keys[i])
		}
	}

}
