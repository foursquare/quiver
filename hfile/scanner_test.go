package hfile

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The sample file `sample/pairs.hfile` has pairs of integers to strings.
// It was generated from the "known-good" scala ver
// The keys are sequential integers represented as 4 bytes (big-endian).
// The values are strings, containing ascii bytes of the string "~x", where x is the key's integer value.
// Thus, the 34th k-v pair has key 00 00 00 1C and value 7E 31 38 ("~18").

var firstSampleKey = MockKeyInt(1)
var firstSampleValue = MockValueInt(1)
var secondSampleBlockKey = []byte{0, 0, 229, 248}

func fakeDataReader(t *testing.T, compress, multi bool) (string, *Reader) {
	f, err := ioutil.TempFile("", "hfile")
	assert.Nil(t, err, "error creating tempfile:", err)

	if multi {
		err = GenerateMockMultiHfile(f.Name(), 100000, 1024*4, compress, false, false)
		assert.Nil(t, err, "cannot write to tempfile: ", err)
	} else {
		err = GenerateMockHfile(f.Name(), 100000, 1024*4, compress, false, false)
		assert.Nil(t, err, "cannot write to tempfile: ", err)
	}
	reader, err := NewReader("sample", f.Name(), CopiedToMem, testing.Verbose())
	assert.Nil(t, err, "error creating reader:", err)
	return f.Name(), reader
}

func TestFirstKeys(t *testing.T) {
	r, err := NewReader("sample", "testdata/pairs.hfile", CopiedToMem, testing.Verbose())
	assert.Nil(t, err, "cannot open sample: ", err)

	assert.True(t, bytes.Equal(r.index[0].firstKeyBytes, firstSampleKey),
		fmt.Sprintf("'%v', expected '%v'\n", r.index[0].firstKeyBytes, firstSampleKey))

	assert.True(t, bytes.Equal(r.index[1].firstKeyBytes, secondSampleBlockKey),
		fmt.Sprintf("'%v', expected '%v'\n", r.index[1].firstKeyBytes, secondSampleBlockKey))
}

func TestGetFirstSample(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	s := r.GetScanner()

	var first, second []byte
	var err error

	first, err, _ = s.GetFirst(MockKeyInt(1))
	assert.Nil(t, err, "error finding key:", err)
	assert.True(t, bytes.Equal(first, MockValueInt(1)),
		fmt.Sprintf("'%v', expected '%v'\n", first, MockValueInt(1)))

	second, err, _ = s.GetFirst(MockKeyInt(1000))
	assert.Nil(t, err, "error finding key:", err)
	assert.True(t, bytes.Equal(second, MockValueInt(1000)),
		fmt.Sprintf("'%v', expected '%v'\n", second, MockValueInt(1000)))

	assert.True(t, bytes.Equal(first, MockValueInt(1)),
		fmt.Sprintf("First value CHANGED '%v', expected '%v'\n", first, MockValueInt(1)))

	second, err, _ = s.GetFirst(MockKeyInt(65547))
	assert.Nil(t, err, "error finding key:", err)
	assert.True(t, bytes.Equal(second, MockValueInt(65547)),
		fmt.Sprintf("'%v', expected '%v'\n", second, MockValueInt(65547)))

	assert.True(t, bytes.Equal(first, MockValueInt(1)),
		fmt.Sprintf("First value CHANGED '%v', expected '%v'\n", first, MockValueInt(1)))
}

func TestMulti(t *testing.T) {
	f, r := fakeDataReader(t, true, true)
	defer os.Remove(f)
	s := r.GetScanner()

	var first, second [][]byte
	expectedFirst := MockMultiValueInt(1, 0)
	var err error

	first, err = s.GetAll(MockKeyInt(1))
	assert.Nil(t, err, "error finding key:", err)
	assert.Len(t, first, 3,
		fmt.Sprintf("wrong number of values for 1: %d", len(first)))

	assert.True(t, bytes.Equal(first[0], expectedFirst),
		fmt.Sprintf("'%v', expected '%v'\n", first[0], expectedFirst))

	second, err = s.GetAll(MockKeyInt(1000))
	assert.Nil(t, err, "error finding key:", err)

	assert.Equal(t, len(second), 1, "wrong number of values for 1000")

	expected := MockValueInt(1000)
	assert.True(t, bytes.Equal(second[0], expected),
		fmt.Sprintf("'%v', expected '%v'\n", second[0], expected))

	assert.True(t, bytes.Equal(first[0], expectedFirst),
		fmt.Sprintf("First value CHANGED '%v', expected '%v'\n", first[0], expectedFirst))

	second, err = s.GetAll(MockKeyInt(1001))
	assert.Nil(t, err, "error finding key:", err)

	assert.Equal(t, len(second), 3, "wrong number of values for 1001")

	expected, actual := MockMultiValueInt(1001, 0), second[0]

	assert.True(t, bytes.Equal(actual, expected),
		fmt.Sprintf("'%v', expected '%v'\n", actual, expected))

	expected, actual = MockMultiValueInt(1001, 2), second[2]

	assert.True(t, bytes.Equal(first[0], expectedFirst),
		fmt.Sprintf("'%v', expected '%v'\n", actual, expected))

	assert.True(t, bytes.Equal(first[0], expectedFirst),
		fmt.Sprintf("First value CHANGED '%v', expected '%v'\n", first[0], expectedFirst))
}
