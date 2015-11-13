// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Map from hex input to (value, len).
type vl struct {
	val int
	len int
}

var testData = map[string]vl{
	"8f8d":               vl{141, 2},
	"8e9999":             vl{0x9999, 3},
	"00":                 vl{0, 1},
	"10":                 vl{0x10, 1},
	"ff":                 vl{-1, 1},
	"7f":                 vl{127, 1},
	"804011111111111111": vl{-286331154, 9}, // 80 size = 9
	"87aa":               vl{-171, 2},       // 87 size = 2
	"885555555555555555": vl{1431655765, 9}, // 80 size = 9
	"8955555555555555":   vl{1431655765, 8},
	"86aaff":             vl{-43776, 3},
	"8faa":               vl{170, 2}, // 8f size = 2
	"90":                 vl{-112, 1},
}

func fromHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	assert.Nil(t, err)
	return b
}

func TestVIntAndLen(t *testing.T) {
	for key, value := range testData {
		x, l := vintAndLen(fromHex(t, key))
		assert.Equal(t, int32(value.val), int32(x))
		assert.Equal(t, value.len, l)
	}
}

func TestVInt(t *testing.T) {
	for key, value := range testData {
		x, err := vint(bytes.NewReader(fromHex(t, key)))
		assert.Nil(t, err)
		assert.Equal(t, int32(value.val), int32(x))
	}
}

func TestVIntErrors(t *testing.T) {
	for key, _ := range testData {
		buf := fromHex(t, key)
		truncatedBuf := buf[0 : len(buf)-1]
		_, err := vint(bytes.NewReader(truncatedBuf))
		assert.NotNil(t, err)
	}
}
