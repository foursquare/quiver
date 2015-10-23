// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fromHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	assert.Nil(t, err)
	return b
}

func TestVInt(t *testing.T) {
	x, l := vintAndLen(fromHex(t, "8f8d"))
	assert.Equal(t, 141, x)
	assert.Equal(t, 2, l)

	x, l = vintAndLen(fromHex(t, "8e9999"))
	assert.Equal(t, 0x9999, x)
	assert.Equal(t, 3, l)

	x, l = vintAndLen(fromHex(t, "00"))
	assert.Equal(t, 0, x)
	assert.Equal(t, 1, l)

	x, l = vintAndLen(fromHex(t, "10"))
	assert.Equal(t, 0x10, x)
	assert.Equal(t, 1, l)

	x, l = vintAndLen(fromHex(t, "ff"))
	assert.Equal(t, -1, x)
	assert.Equal(t, 1, l)

	x, l = vintAndLen(fromHex(t, "7f"))
	assert.Equal(t, 127, x)
	assert.Equal(t, 1, l)

	// 80 size = 9
	x, l = vintAndLen(fromHex(t, "804011111111111111"))
	assert.Equal(t, int32(-286331154), int32(x))
	assert.Equal(t, 9, l)

	// 87 size = 2
	x, l = vintAndLen(fromHex(t, "87aa"))
	assert.Equal(t, -171, x)
	assert.Equal(t, 2, l)

	// 80 size = 9
	x, l = vintAndLen(fromHex(t, "885555555555555555"))
	assert.Equal(t, int32(1431655765), int32(x))
	assert.Equal(t, 9, l)

	x, l = vintAndLen(fromHex(t, "8955555555555555"))
	assert.Equal(t, int32(1431655765), int32(x))
	assert.Equal(t, 8, l)

	x, l = vintAndLen(fromHex(t, "86aaff"))
	assert.Equal(t, -43776, x)
	assert.Equal(t, 3, l)

	// 8f size = 2
	x, l = vintAndLen(fromHex(t, "8faa"))
	assert.Equal(t, 170, x)
	assert.Equal(t, 2, l)

	x, l = vintAndLen(fromHex(t, "90"))
	assert.Equal(t, -112, x)
	assert.Equal(t, 1, l)

}
