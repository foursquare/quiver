// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"log"
)

type Scanner struct {
	reader *Reader
	idx    int
	block  []byte
	pos    *int
	buf    []byte

	// When off, maybe be faster but may return incorrect results rather than error on out-of-order keys.
	EnforceKeyOrder bool
	OrderedOps
}

func NewScanner(r *Reader) *Scanner {
	var buf []byte
	if r.CompressionCodec > CompressionNone {
		buf = make([]byte, int(float64(r.TotalUncompressedDataBytes/uint64(len(r.index)))*1.5))
	}
	return &Scanner{r, 0, nil, nil, buf, true, OrderedOps{nil}}
}

func (s *Scanner) Reset() {
	s.idx = 0
	s.block = nil
	s.pos = nil
	s.ResetState()
}

func (s *Scanner) blockFor(key []byte) ([]byte, error, bool) {
	if s.EnforceKeyOrder {
		err := s.CheckIfKeyOutOfOrder(key)
		if err != nil {
			return nil, err, false
		}
	}

	if s.reader.index[s.idx].IsAfter(key) {
		if s.reader.Debug {
			log.Printf("[Scanner.blockFor] curBlock after key %s (cur: %d, start: %s)\n",
				hex.EncodeToString(key),
				s.idx,
				hex.EncodeToString(s.reader.index[s.idx].firstKeyBytes),
			)
		}
		return nil, nil, false
	}

	idx := s.reader.FindBlock(s.idx, key)
	if s.reader.Debug {
		log.Printf("[Scanner.blockFor] findBlock (key: %s) picked %d (starts: %s). Cur: %d (starts: %s)\n",
			hex.EncodeToString(key),
			idx,
			hex.EncodeToString(s.reader.index[idx].firstKeyBytes),
			s.idx,
			hex.EncodeToString(s.reader.index[s.idx].firstKeyBytes),
		)
	}

	if idx != s.idx || s.block == nil { // need to load a new block
		data, err := s.reader.GetBlockBuf(idx, s.buf)
		if err != nil {
			if s.reader.Debug {
				log.Printf("[Scanner.blockFor] read err %s (key: %s, idx: %d, start: %s)\n",
					err,
					hex.EncodeToString(key),
					idx,
					hex.EncodeToString(s.reader.index[idx].firstKeyBytes),
				)
			}
			return nil, err, false
		}
		i := 8
		s.pos = &i
		s.idx = idx
		s.block = data
	} else {
		if s.reader.Debug {
			log.Println("[Scanner.blockFor] Re-using current block")
		}
	}

	return s.block, nil, true
}

func (s *Scanner) GetFirst(key []byte) ([]byte, error, bool) {
	data, err, ok := s.blockFor(key)

	if !ok {
		if s.reader.Debug {
			log.Printf("[Scanner.GetFirst] No Block for key: %s (err: %s, found: %s)\n", hex.EncodeToString(key), err, ok)
		}
		return nil, err, ok
	}

	if s.reader.Debug {
		log.Printf("[Scanner.GetFirst] Searching Block for key: %s (pos: %d)\n", hex.EncodeToString(key), *s.pos)
	}
	value, _, found := s.getValuesFromBuffer(data, s.pos, key, true)
	if s.reader.Debug {
		log.Printf("[Scanner.GetFirst] After pos pos: %d\n", *s.pos)
	}
	return value, nil, found
}

func (s *Scanner) GetAll(key []byte) ([][]byte, error) {
	data, err, ok := s.blockFor(key)

	if !ok {
		if s.reader.Debug {
			log.Printf("[Scanner.GetAll] No Block for key: %s (err: %s, found: %s)\n", hex.EncodeToString(key), err, ok)
		}
		return nil, err
	}

	_, found, _ := s.getValuesFromBuffer(data, s.pos, key, false)
	return found, err
}

func (s *Scanner) getValuesFromBuffer(buf []byte, pos *int, key []byte, first bool) ([]byte, [][]byte, bool) {
	var acc [][]byte

	i := *pos

	if s.reader.Debug {
		log.Printf("[Scanner.getValuesFromBuffer] buf before %d / %d\n", i, len(buf))
	}

	for len(buf)-i > 8 {
		keyLen := int(binary.BigEndian.Uint32(buf[i : i+4]))
		valLen := int(binary.BigEndian.Uint32(buf[i+4 : i+8]))

		cmp := bytes.Compare(buf[i+8:i+8+keyLen], key)

		switch {
		case cmp == 0:
			i += 8 + keyLen

			ret := make([]byte, valLen)
			copy(ret, buf[i:i+valLen])

			i += valLen // now on next length pair

			if first {
				*pos = i
				return ret, nil, true
			}
			acc = append(acc, ret)
		case cmp > 0:
			*pos = i
			return nil, acc, len(acc) > 0
		default:
			i += 8 + keyLen + valLen
		}
	}

	*pos = i

	if s.reader.Debug {
		log.Printf("[Scanner.getValuesFromBuffer] walked off block\n")
	}
	return nil, acc, len(acc) > 0
}

func (s *Scanner) Release() {
	s.Reset()
	select {
	case s.reader.scannerCache <- s:
	default:
	}
}
