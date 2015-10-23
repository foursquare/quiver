// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()
	ok, err := i.Next()

	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.Equal(t, MockKeyInt(0), i.Key())
	assert.Equal(t, MockValueInt(0), i.Value())

	ok, err = i.Next()
	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.Equal(t, MockKeyInt(1), i.Key())
	assert.Equal(t, MockValueInt(1), i.Value())

	ok, err = i.Seek(MockKeyInt(65537))
	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.Equal(t, MockKeyInt(65537), i.Key())
	assert.Equal(t, MockValueInt(65537), i.Value())

	ok, err = i.Seek(MockKeyInt(75537))
	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.Equal(t, MockKeyInt(75537), i.Key())
	assert.Equal(t, MockValueInt(75537), i.Value())
}

func TestSinglePrefix(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()

	res, last, err := i.AllForPrefixes([][]byte{[]byte{0, 0, 1}}, 0, nil)
	assert.Nil(t, err, "error finding all for prefixes:", err)

	assert.Len(t, res, 256, "Wrong number of matched keys")

	k := string(MockKeyInt(511))
	v, ok := res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, [][]byte{MockValueInt(511)}, v)

	k = string(MockKeyInt(256))
	v, ok = res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, [][]byte{MockValueInt(256)}, v)

	k = string([]byte{0, 0, 0, 255})
	_, ok = res[k]
	assert.False(t, ok, fmt.Sprintf("Key %v should not be in res %v", k, res))

	k = string([]byte{0, 0, 2, 0})
	_, ok = res[k]
	assert.False(t, ok, fmt.Sprintf("Key %v should not be in res %v", k, res))

	k = string([]byte{0, 0, 1, 30})
	v, ok = res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, MockValueInt(286), v[0])

	assert.Nil(t, last)
}

func TestSinglePrefixWithLimit(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()

	limit := int32(10)
	res, last, err := i.AllForPrefixes([][]byte{[]byte{0, 0, 1}}, limit, nil)
	assert.Nil(t, err, "error finding all for prefixes:", err)

	assert.Len(t, res, int(limit), "Wrong number of matched keys")

	k := string(MockKeyInt(256))
	v, ok := res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, [][]byte{MockValueInt(256)}, v)

	k = string(MockKeyInt(265))
	v, ok = res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, [][]byte{MockValueInt(265)}, v)

	assert.Equal(t, MockKeyInt(266), last)

	k = string(MockKeyInt(266))
	_, ok = res[k]
	assert.False(t, ok, fmt.Sprintf("Key %v should not be in res %v", k, res))
}

func TestSinglePrefixWithLimitAndLastKey(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()

	limit := int32(10)
	res, last, err := i.AllForPrefixes([][]byte{[]byte{0, 0, 1}}, limit, []byte{0, 0, 1, 100})

	assert.Equal(t, MockKeyInt(366), last)

	assert.Nil(t, err, "error finding all for prefixes:", err)

	assert.Len(t, res, int(limit), "Wrong number of matched keys")

	k := string(MockKeyInt(356))
	v, ok := res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, [][]byte{MockValueInt(356)}, v)

	k = string(MockKeyInt(365))
	v, ok = res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Equal(t, [][]byte{MockValueInt(365)}, v)

	k = string(MockKeyInt(366))
	_, ok = res[k]
	assert.False(t, ok, fmt.Sprintf("Key %v should not be in res %v", k, res))
}
