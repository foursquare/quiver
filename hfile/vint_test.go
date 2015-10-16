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

	x, l = vintAndLen(fromHex(t, "a0"))
	assert.Equal(t, -96, x)
	assert.Equal(t, 1, l)

	x, l = vintAndLen(fromHex(t, "a0"))
	assert.Equal(t, -96, x)
	assert.Equal(t, 1, l)

	x, l = vintAndLen(fromHex(t, "a0"))
	assert.Equal(t, -96, x)
	assert.Equal(t, 1, l)

	x, l = vintAndLen(fromHex(t, "87aa"))
	assert.Equal(t, -171, x)
	assert.Equal(t, 2, l)

	x, l = vintAndLen(fromHex(t, "86aaff"))
	assert.Equal(t, -43776, x)
	assert.Equal(t, 3, l)

}
