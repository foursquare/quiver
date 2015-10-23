// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
)

type Iterator struct {
	hfile          *Reader
	dataBlockIndex int

	block []byte
	pos   int

	buf []byte

	key   []byte
	value []byte
	OrderedOps
}

func NewIterator(r *Reader) *Iterator {
	var buf []byte
	if r.compressionCodec > CompressionNone {
		buf = make([]byte, int(float64(r.totalUncompressedDataBytes/uint64(len(r.index)))*1.5))
	}

	it := Iterator{r, 0, nil, 0, buf, nil, nil, OrderedOps{nil}}
	return &it
}

func (it *Iterator) Reset() {
	it.dataBlockIndex = 0
	it.block = nil
	it.pos = 0
	it.key = nil
	it.value = nil
	it.ResetState()
}

/*
  Position the iterator at-or-after requested key by seeking forward from current position.

  If already at-or-after, this is a no-op.
  Otherwise, if the requested key exists, it will be positioned there, otherwise at the first key
  greater than the requested one.
  Returns true if, after positioning, the iterator is on a valid key (eg not at EOF), same as Next.
*/
func (it *Iterator) Seek(requested []byte) (bool, error) {
	var err error

	if err = it.CheckIfKeyOutOfOrder(requested); err != nil {
		return false, err
	}

	if it.key != nil && !After(requested, it.key) {
		if it.hfile.Debug {
			log.Println("[Iterator.Seek] already at or past requested")
		}
		return true, nil
	}

	blk := it.hfile.FindBlock(it.dataBlockIndex, requested)

	if it.hfile.Debug {
		log.Printf("[Iterator.Seek] picked block %d, cur %d\n", blk, it.dataBlockIndex)
		if len(it.hfile.index) > blk+1 {
			log.Printf("[Iterator.Seek] following block starts at: %v\n", it.hfile.index[blk+1].firstKeyBytes)
		} else {
			log.Printf("[Iterator.Seek] (last block)\n")
		}
	}

	if blk != it.dataBlockIndex { // if, and only if, FindBlock returned a block other than current.
		if it.hfile.Debug {
			log.Println("[Iterator.Seek] new block!")
		}
		it.dataBlockIndex = blk
		it.block = nil // make Next load the new block.
	}

	ok, err := it.Next()
	if err != nil {
		return false, err
	}

	if it.hfile.Debug {
		log.Printf("[Iterator.Seek] looking for %v (starting at %v)\n", requested, it.key)
	}

	for ok {
		after := After(requested, it.key) // the key we are looking for is in our future.

		if it.hfile.Debug {
			log.Printf("[Iterator.Seek] \t %v (%v)\n", it.key, after)
		}

		if err == nil && after { // we still need to go forward.
			ok, err = it.Next()
		} else {
			// either we got an error or we no longer need to go forward.
			if it.hfile.Debug {
				log.Printf("[Iterator.Seek] done %v (err %v)\n", it.key, err)
			}
			return ok, err
		}
	}
	if it.hfile.Debug {
		log.Println("[Iterator.Seek] walked off block")
	}

	return ok, err
}

/*
  Load a kv pair into it.key/it.value and advance the iterator.
  Return true if a kv pair was loaded (eg can call Key), or false if at EOF.
*/
func (it *Iterator) Next() (bool, error) {
	var err error

	it.key = nil
	it.value = nil

	if it.dataBlockIndex >= len(it.hfile.index) { // EOF, we're out of blocks.
		return false, nil
	}

	if it.block == nil { // current block has not been loaded yet.
		it.block, err = it.hfile.GetBlockBuf(it.dataBlockIndex, it.buf)
		if err != nil {
			return false, err
		}
		it.pos = len(DataMagic) // skip the magic bytes
	}

	if len(it.block)-it.pos <= 0 { // nothing left in this block to read, need a new block.
		it.dataBlockIndex += 1
		it.block = nil // will cause loading of the block in the next call.
		return it.Next()
	}

	// 4 bytes each for key and value lengths, so some pointer arithmatic.
	keyLen := int(binary.BigEndian.Uint32(it.block[it.pos : it.pos+4]))
	valLen := int(binary.BigEndian.Uint32(it.block[it.pos+4 : it.pos+8]))
	it.pos += 8

	// it.pos now sitting on the begining of the key
	it.key = it.block[it.pos : it.pos+keyLen]
	it.value = it.block[it.pos+keyLen : it.pos+keyLen+valLen]
	it.pos += keyLen + valLen // move position to next kv pair.
	return true, nil
}

/*
  A copy of the current key
  it.key is a pointer into a buffer that may get recycled for another block, thus the copy.
*/
func (it *Iterator) Key() []byte {
	ret := make([]byte, len(it.key))
	copy(ret, it.key)
	return ret
}

/*
  A copy of the current value
  it.key is a pointer into a buffer that may get recycled for another block, thus the copy.
*/
func (it *Iterator) Value() []byte {
	ret := make([]byte, len(it.value))
	copy(ret, it.value)
	return ret
}

func (it *Iterator) AllForPrefixes(prefixes [][]byte, limit int32, lastKey []byte) (map[string][][]byte, []byte, error) {
	if limit <= 0 {
		limit = math.MaxInt32
	}
	res := make(map[string][][]byte)
	values := int32(0)
	var err error

	preseekOk := false
	if lastKey != nil {
		if preseekOk, err = it.Seek(lastKey); err != nil {
			return nil, nil, err
		}
	}

	for _, prefix := range prefixes {
		ok := false

		if lastKey == nil || bytes.Compare(lastKey, prefix) <= 0 {
			if ok, err = it.Seek(prefix); err != nil {
				return nil, nil, err
			}
		} else {
			ok = preseekOk
		}

		acc := make([][]byte, 0, 1)

		for ok && bytes.HasPrefix(it.key, prefix) {
			prev := it.key
			acc = append(acc, it.Value())

			if ok, err = it.Next(); err != nil {
				return nil, nil, err
			}
			values++

			if !ok || !bytes.Equal(prev, it.key) {
				cp := make([]byte, len(prev))
				copy(cp, prev)
				res[string(cp)] = acc
				acc = make([][]byte, 0, 1)
				if values >= limit {
					var last []byte
					// reached limit and at a key boundry or end-of-file.
					// We _are_ going to return now, but first may need to copy next key to `last` if it is a match.
					if ok && bytes.HasPrefix(it.key, prefix) {
						last = make([]byte, len(it.key))
						copy(last, it.key)
					}
					return res, last, nil
				}
			}
		}
		if !ok {
			break
		}
	}

	return res, nil, nil
}

func (it *Iterator) Release() {
	it.Reset()
	select {
	case it.hfile.iteratorCache <- it:
	default:
	}
}
