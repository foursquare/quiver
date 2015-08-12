// Copyright (C) 2014 Daniel Harrison

package main

import (
	"bytes"
	"log"
	"sort"
)
import "encoding/binary"
import "errors"
import "github.com/edsrzf/mmap-go"
import "os"

type HfileReader struct {
	mmap      mmap.MMap
	version   Version
	header    Header
	dataIndex DataIndex
	cur       int
	lastKey   *[]byte
}

func NewHfileReader(file *os.File) (HfileReader, error) {
	hfile := HfileReader{}
	var err error
	hfile.mmap, err = mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return hfile, err
	}

	versionIndex := len(hfile.mmap) - 4
	hfile.version, err = newVersion(bytes.NewReader(hfile.mmap[versionIndex:]))
	hfile.header, err = hfile.version.newHeader(hfile.mmap)
	hfile.dataIndex, err = hfile.header.newDataIndex(hfile.mmap)

	return hfile, nil
}

func (hfile *HfileReader) String() string {
	return "hfile"
}

func (r *HfileReader) blockFor(key []byte) (*DataBlock, bool) {
	if r.lastKey != nil && bytes.Compare(key, *r.lastKey) < 0 {
		r.dataIndex.dataBlocks[r.cur].reset()
		r.cur = 0
	}
	r.lastKey = &key

	if bytes.Compare(r.dataIndex.dataBlocks[r.cur].firstKeyBytes, key) >= 0 {
		return &r.dataIndex.dataBlocks[r.cur], true
	}

	lim := len(r.dataIndex.dataBlocks) - r.cur
	i := sort.Search(lim, func(i int) bool {
		return bytes.Compare(r.dataIndex.dataBlocks[r.cur+i].firstKeyBytes, key) > 0
	})

	if i == 0 {
		return nil, false
	}

	r.cur = r.cur + i - 1

	r.dataIndex.dataBlocks[r.cur].reset()

	return &r.dataIndex.dataBlocks[r.cur], true
}

func (hfile *HfileReader) GetFirst(key []byte) ([]byte, bool) {
	dataBlock, ok := hfile.blockFor(key)

	if !ok {
		log.Println("no block for key ", key)
		return nil, false
	}

	value, _, found := dataBlock.get(key, true)
	return value, found
}

func (hfile *HfileReader) GetAll(key []byte) [][]byte {
	dataBlock, ok := hfile.blockFor(key)

	if !ok {
		log.Println("no block for key ", key)
		return nil
	}

	_, found, _ := dataBlock.get(key, false)
	return found
}

type Version struct {
	buf          *bytes.Reader
	majorVersion uint32
	minorVersion uint32
}

func newVersion(versionBuf *bytes.Reader) (Version, error) {
	version := Version{buf: versionBuf}
	var rawByte uint32
	binary.Read(version.buf, binary.BigEndian, &rawByte)
	version.majorVersion = rawByte & 0x00ffffff
	version.minorVersion = rawByte >> 24
	return version, nil
}
func (version *Version) newHeader(mmap mmap.MMap) (Header, error) {
	header := Header{}

	if version.majorVersion != 1 || version.minorVersion != 0 {
		return header, errors.New("wrong version")
	}

	header.index = len(mmap) - 60
	header.buf = bytes.NewReader(mmap[header.index:])
	headerMagic := make([]byte, 8)
	header.buf.Read(headerMagic)
	if bytes.Compare(headerMagic, []byte("TRABLK\"$")) != 0 {
		return header, errors.New("bad header magic")
	}

	binary.Read(header.buf, binary.BigEndian, &header.fileInfoOffset)
	binary.Read(header.buf, binary.BigEndian, &header.dataIndexOffset)
	binary.Read(header.buf, binary.BigEndian, &header.dataIndexCount)
	binary.Read(header.buf, binary.BigEndian, &header.metaIndexOffset)
	binary.Read(header.buf, binary.BigEndian, &header.metaIndexCount)
	binary.Read(header.buf, binary.BigEndian, &header.totalUncompressedDataBytes)
	binary.Read(header.buf, binary.BigEndian, &header.entryCount)
	binary.Read(header.buf, binary.BigEndian, &header.compressionCodec)
	return header, nil
}

type Header struct {
	buf   *bytes.Reader
	index int

	fileInfoOffset             uint64
	dataIndexOffset            uint64
	dataIndexCount             uint32
	metaIndexOffset            uint64
	metaIndexCount             uint32
	totalUncompressedDataBytes uint64
	entryCount                 uint32
	compressionCodec           uint32
}

func (header *Header) newDataIndex(mmap mmap.MMap) (DataIndex, error) {
	dataIndex := DataIndex{}
	dataIndexEnd := header.metaIndexOffset
	if header.metaIndexOffset == 0 {
		dataIndexEnd = uint64(header.index)
	}
	dataIndex.buf = bytes.NewReader(mmap[header.dataIndexOffset:dataIndexEnd])

	dataIndexMagic := make([]byte, 8)
	dataIndex.buf.Read(dataIndexMagic)
	if bytes.Compare(dataIndexMagic, []byte("IDXBLK)+")) != 0 {
		return dataIndex, errors.New("bad data index magic")
	}

	for dataIndex.buf.Len() > 0 {
		dataBlock := DataBlock{idx: len(dataIndex.dataBlocks)}

		binary.Read(dataIndex.buf, binary.BigEndian, &dataBlock.offset)
		binary.Read(dataIndex.buf, binary.BigEndian, &dataBlock.size)

		dataBlock.buf = bytes.NewReader(mmap[dataBlock.offset : dataBlock.offset+uint64(dataBlock.size)])
		dataBlockMagic := make([]byte, 8)
		dataBlock.buf.Read(dataBlockMagic)
		if bytes.Compare(dataBlockMagic, []byte("DATABLK*")) != 0 {
			return dataIndex, errors.New("bad data block magic")
		}

		firstKeyLen, _ := binary.ReadUvarint(dataIndex.buf)
		dataBlock.firstKeyBytes = make([]byte, firstKeyLen)
		dataIndex.buf.Read(dataBlock.firstKeyBytes)

		dataIndex.dataBlocks = append(dataIndex.dataBlocks, dataBlock)
	}

	return dataIndex, nil
}

type DataIndex struct {
	buf        *bytes.Reader
	dataBlocks []DataBlock
}

type DataBlock struct {
	idx           int
	buf           *bytes.Reader
	offset        uint64
	size          uint32
	firstKeyBytes []byte
}

func (dataBlock *DataBlock) reset() {
	dataBlock.buf.Seek(8, 0)
}

func (dataBlock *DataBlock) get(key []byte, first bool) ([]byte, [][]byte, bool) {
	var acc [][]byte

	for dataBlock.buf.Len() > 0 {
		var keyLen, valLen uint32
		binary.Read(dataBlock.buf, binary.BigEndian, &keyLen)
		binary.Read(dataBlock.buf, binary.BigEndian, &valLen)
		keyBytes := make([]byte, keyLen)
		valBytes := make([]byte, valLen)
		dataBlock.buf.Read(keyBytes)
		dataBlock.buf.Read(valBytes)
		cmp := bytes.Compare(key, keyBytes)
		if cmp == 0 {
			if first {
				return valBytes, acc, true
			} else {
				acc = append(acc, valBytes)
			}
		}
		if cmp < 0 {
			return nil, acc, false
		}
	}
	return nil, nil, false
}
