package hfile

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func tempHfile(t *testing.T, compress bool, blockSize int, keys [][]byte, values [][]byte) (string, *Scanner) {
	fp, err := ioutil.TempFile("", "demohfile")
	assert.Nil(t, err, "error creating tempfile:", err)

	if testing.Verbose() {
		log.Println("###############")
		log.Println("Writing temp hfile:", fp.Name())
		log.Println("###############")
	}
	w, err := NewWriter(fp, compress, blockSize, testing.Verbose())
	assert.Nil(t, err, "error creating writer:", err)

	for i, _ := range keys {
		err := w.Write(keys[i], values[i])
		assert.Nil(t, err, "error writing k-v pair:", err)
	}
	w.Close()

	if testing.Verbose() {
		log.Println("###############")
		log.Println("Done writing temp hfile:", fp.Name())
		log.Println("###############")
	}

	r, err := NewReader("demo", fp.Name(), CopiedToMem, testing.Verbose())
	assert.Nil(t, err, "error creating reader:", err)

	s := NewScanner(r)

	return fp.Name(), s
}

func keyI(i int) []byte {
	return MockKeyInt(i)
}

func valI(i int) []byte {
	return MockValueInt(i)
}

func TestRoundTrip(t *testing.T) {
	keys := [][]byte{keyI(1), keyI(2), keyI(3), keyI(4)}
	vals := [][]byte{valI(1), valI(2), valI(3), valI(4)}

	f, s := tempHfile(t, false, 4096, keys, vals)
	defer os.Remove(f)

	v, err, found := s.GetFirst(keyI(3))
	assert.Nil(t, err, err)
	assert.True(t, found, "not found")

	assert.True(t, bytes.Equal(v, valI(3)), "bad value", v, valI(3))

	v, err, found = s.GetFirst(keyI(5))
	assert.Nil(t, err, err)
	assert.False(t, found, "missing key should not have been found.")
}

func TestRoundTripCompressed(t *testing.T) {
	keys := [][]byte{keyI(1), keyI(2), keyI(3), keyI(4)}
	vals := [][]byte{valI(1), valI(2), valI(3), valI(4)}

	f, s := tempHfile(t, true, 4096, keys, vals)
	defer os.Remove(f)

	v, err, found := s.GetFirst(keyI(3))
	assert.Nil(t, err, err)
	assert.True(t, found, "not found")

	assert.True(t, bytes.Equal(v, valI(3)), "bad value", v, valI(3))

	v, err, found = s.GetFirst(keyI(5))
	assert.Nil(t, err, err)
	assert.False(t, found, "missing key should not have been found.")
}

func TestMultiValueRoundTripCompressed(t *testing.T) {
	keys := [][]byte{keyI(10), keyI(10), keyI(20), keyI(30), keyI(30), keyI(30), keyI(40)}
	vals := [][]byte{valI(10), valI(11), valI(20), valI(30), valI(31), valI(32), valI(40)}

	f, s := tempHfile(t, true, 4096, keys, vals)
	defer os.Remove(f)

	v, err := s.GetAll(keyI(30))
	assert.Nil(t, err, err)
	assert.Len(t, v, 3, "wrong number of values for key 30", len(v))

	assert.True(t, bytes.Equal(v[1], valI(31)), "bad value for key 30 (1)", v[1], valI(31))

	v, err = s.GetAll(keyI(40))
	assert.Nil(t, err, err)
	assert.Len(t, v, 1, "wrong number of results for key 40", len(v))

	assert.True(t, bytes.Equal(v[0], valI(40)), "bad value for key 40", v[0], valI(40))

	v, err = s.GetAll(keyI(50))
	assert.Nil(t, err, err)
	assert.Len(t, v, 0, "should not find missing keys")
}

func TestBigRoundTripCompressed(t *testing.T) {
	keys := make([][]byte, 1000)
	vals := make([][]byte, 1000)

	for i, _ := range keys {
		keys[i] = keyI(i)
		vals[i] = valI(i)
	}

	f, s := tempHfile(t, true, 4096, keys, vals)
	defer os.Remove(f)

	v, err, found := s.GetFirst(keyI(501))
	assert.Nil(t, err, err)
	assert.True(t, found, "not found")
	assert.True(t, bytes.Equal(v, valI(501)), "bad value", v, valI(501))
}
