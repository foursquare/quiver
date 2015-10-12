package lru

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRU(t *testing.T) {
	b1 := []byte{1}
	b2 := []byte{2}
	b3 := []byte{3}
	b4 := []byte{4}

	lru := NewLRU(3)

	_, ok := lru.Get(1)
	assert.False(t, ok, "empty lru should return nothing")

	lru.Add(1, b1)

	_, ok = lru.Get(1)
	assert.True(t, ok, "missing only item")

	lru.Add(2, b2)
	lru.Add(3, b3)

	_, ok = lru.Get(1)
	assert.True(t, ok, "missing only item")
	_, ok = lru.Get(2)
	assert.True(t, ok, "missing only item")
	_, ok = lru.Get(3)
	assert.True(t, ok, "missing only item")
	_, ok = lru.Get(4)
	assert.False(t, ok, "phantom item")

	lru.Add(4, b4)
	_, ok = lru.Get(1)
	assert.False(t, ok, "phantom item")
	_, ok = lru.Get(4)
	assert.True(t, ok, "missing item 4")
}
