package util

import (
	"bytes"
	"testing"
)

func DoubleEq(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, _ := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestProduct(t *testing.T) {
	b1 := []byte{1}
	b2 := []byte{2}
	b3 := []byte{3}
	b4 := []byte{4}

	chunk1 := [][]byte{b1, b2}
	chunk2 := [][]byte{b3, b4}

	chunks := [][][]byte{chunk1, chunk2}

	p := RevProduct(chunks)
	if len(p) != 4 {
		t.Logf("products: %v", p)
		t.Fatalf("wrong number of products: %d (expected %d)", len(p), 4)
	}
	if !DoubleEq(p[0], [][]byte{b1, b3}) {
		t.Fatalf("wrong product at pos 0: %v instead of %v", p[0], [][]byte{b1, b3})
	}
	if !DoubleEq(p[1], [][]byte{b1, b4}) {
		t.Fatalf("wrong product at pos 1: %v instead of %v", p[1], [][]byte{b1, b4})
	}
	if !DoubleEq(p[2], [][]byte{b2, b3}) {
		t.Fatalf("wrong product at pos 2: %v instead of %v", p[2], [][]byte{b2, b3})
	}
	if !DoubleEq(p[3], [][]byte{b2, b4}) {
		t.Fatalf("wrong product at pos 3: %v instead of %v", p[3], [][]byte{b2, b4})
	}
}
