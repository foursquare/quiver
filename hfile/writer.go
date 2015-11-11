// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"

	"github.com/golang/snappy"
)

type Writer struct {
	blockSizeLimit int

	debug bool

	fp        io.WriteCloser
	curOffset uint64

	curBlockBuf      *bytes.Buffer
	curBlockFirstKey []byte

	blocks  []Block
	trailer Trailer

	OrderedOps
}

func NewLocalWriter(path string, compress bool, blockSize int, debug bool) (*Writer, error) {
	fp, err := os.Create(path)

	if err != nil {
		return nil, err
	}

	return NewWriter(fp, compress, blockSize, debug)
}

func NewWriter(out io.WriteCloser, compress bool, blockSize int, debug bool) (*Writer, error) {
	w := new(Writer)
	w.fp = out
	w.debug = debug
	w.blockSizeLimit = blockSize
	w.OrderedOps = OrderedOps{nil}

	if compress {
		w.trailer.CompressionCodec = CompressionSnappy
	} else {
		w.trailer.CompressionCodec = CompressionNone
	}

	return w, nil
}

func (w *Writer) Write(k, v []byte) error {
	w.maybeStartBlock(k)

	if err := w.CheckIfKeyOutOfOrder(k); err != nil {
		return err
	}

	if err := binary.Write(w.curBlockBuf, binary.BigEndian, uint32(len(k))); err != nil {
		return err
	}
	if err := binary.Write(w.curBlockBuf, binary.BigEndian, uint32(len(v))); err != nil {
		return err
	}
	if _, err := w.curBlockBuf.Write(k); err != nil {
		return err
	}
	if _, err := w.curBlockBuf.Write(v); err != nil {
		return err
	}
	w.trailer.EntryCount += 1
	return nil
}

func (w *Writer) maybeStartBlock(key []byte) error {
	if w.curBlockBuf != nil && w.curBlockBuf.Len() >= w.blockSizeLimit {
		if w.debug {
			log.Printf("[Writer.maybeStartBlock] block %d is full (%db)", len(w.blocks), w.curBlockBuf.Len())
		}

		if w.Same(key) {
			if w.debug {
				log.Printf("[Writer.maybeStartBlock] Waiting for new key to split block (%s)", hex.EncodeToString(key))
			}
			return nil
		}

		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if w.curBlockBuf == nil {
		if w.debug {
			log.Printf("[Writer.maybeStartBlock] Starting new block %d @ key %s", len(w.blocks), hex.EncodeToString(key))
		}
		arr := make([]byte, 0, w.blockSizeLimit)
		w.curBlockBuf = bytes.NewBuffer(arr)
		if _, err := w.curBlockBuf.Write(DataMagic); err != nil {
			return err
		}
		w.curBlockFirstKey = key
	}

	return nil
}

func (w *Writer) Close() error {
	if w.curBlockBuf != nil {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if err := w.flushFileInfo(); err != nil {
		return err
	}

	if err := w.flushIndex(); err != nil {
		return err
	}

	if err := w.flushMetaIndex(); err != nil {
		return err
	}

	if err := w.flushTrailer(); err != nil {
		return err
	}

	if err := binary.Write(w.fp, binary.BigEndian, uint32(1)); err != nil {
		return err
	}

	w.fp.Close()
	return nil
}

func (w *Writer) flushBlock() error {
	if w.debug {
		log.Printf("[Writer.flushBlock] flushing block %d (%d keys, %db)", len(w.blocks), w.trailer.EntryCount, w.curBlockBuf.Len())
	}
	block := Block{w.curOffset, uint32(w.curBlockBuf.Len()), w.curBlockFirstKey}
	w.trailer.TotalUncompressedDataBytes += uint64(w.curBlockBuf.Len())

	switch w.trailer.CompressionCodec {
	case CompressionNone:
		if i, err := w.curBlockBuf.WriteTo(w.fp); err != nil {
			return err
		} else {
			w.curOffset += uint64(i)
		}

	case CompressionSnappy:
		fullSz := uint32(w.curBlockBuf.Len())
		if err := binary.Write(w.fp, binary.BigEndian, fullSz); err != nil {
			return err
		} else {
			w.curOffset += uint64(binary.Size(fullSz))
		}
		compressed := snappy.Encode(nil, w.curBlockBuf.Bytes())
		if w.debug {
			log.Printf("[Writer.flushBlock] compressed block %d (%db -> %db)", len(w.blocks), w.curBlockBuf.Len(), len(compressed))
		}

		compressedSz := uint32(len(compressed))
		if err := binary.Write(w.fp, binary.BigEndian, compressedSz); err != nil {
			return err
		} else {
			w.curOffset += uint64(binary.Size(compressedSz))
		}

		if i, err := w.fp.Write(compressed); err != nil {
			return err
		} else {
			w.curOffset += uint64(i)
		}

	default:
		return errors.New("Unsupported compression codec " + string(w.trailer.CompressionCodec))
	}

	w.blocks = append(w.blocks, block)
	w.curBlockBuf = nil
	return nil
}

func writeUvarint(fp io.Writer, i uint64) (int, error) {
	buf := make([]byte, 16)
	l := binary.PutUvarint(buf, uint64(i))
	return fp.Write(buf[0:l])
}

func (w *Writer) flushIndex() error {
	w.trailer.DataIndexOffset = w.curOffset
	w.trailer.DataIndexCount = uint32(len(w.blocks))

	w.fp.Write(IndexMagic)
	w.curOffset += uint64(len(IndexMagic))

	for _, b := range w.blocks {
		if err := binary.Write(w.fp, binary.BigEndian, b.offset); err != nil {
			return err
		}
		w.curOffset += uint64(binary.Size(b.offset))

		if err := binary.Write(w.fp, binary.BigEndian, b.size); err != nil {
			return err
		}
		w.curOffset += uint64(binary.Size(b.size))

		if i, err := writeUvarint(w.fp, uint64(len(b.firstKeyBytes))); err != nil {
			return err
		} else {
			w.curOffset += uint64(i)
		}

		if i, err := w.fp.Write(b.firstKeyBytes); err != nil {
			return err
		} else {
			w.curOffset += uint64(i)
		}
	}

	return nil
}

func (w *Writer) flushFileInfo() error {
	w.trailer.FileInfoOffset = w.curOffset
	//TODO(davidt): support file info
	return nil
}

func (w *Writer) flushMetaIndex() error {
	w.trailer.MetaIndexOffset = w.curOffset
	w.trailer.MetaIndexCount = uint32(0)
	return nil
}

func (w *Writer) flushTrailer() error {
	w.fp.Write(TrailerMagic)

	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.FileInfoOffset); err != nil {
		return err
	}

	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.DataIndexOffset); err != nil {
		return err
	}
	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.DataIndexCount); err != nil {
		return err
	}

	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.MetaIndexOffset); err != nil {
		return err
	}
	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.MetaIndexCount); err != nil {
		return err
	}

	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.TotalUncompressedDataBytes); err != nil {
		return err
	}
	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.EntryCount); err != nil {
		return err
	}
	if err := binary.Write(w.fp, binary.BigEndian, w.trailer.CompressionCodec); err != nil {
		return err
	}
	return nil
}
