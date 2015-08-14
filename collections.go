package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/dt/thile/gen"
	"github.com/paperstreet/gohfile/hfile"
)

type Collection struct {
	Name string
	Path string
}

type CollectionSet struct {
	readers map[string]*hfile.Reader
}

func LoadCollections(collections []Collection, lock bool) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.readers = make(map[string]*hfile.Reader)

	for _, config := range collections {

		f, err := os.OpenFile(config.Path, os.O_RDONLY, 0)

		if err != nil {
			return nil, err
		}

		reader, err := hfile.NewReader(config.Name, f, lock)
		if err != nil {
			return nil, err
		}

		cs.readers[config.Name] = reader
	}

	return cs, nil
}

func (cs *CollectionSet) readerFor(c string) (*hfile.Reader, error) {
	reader, ok := cs.readers[c]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", c)
	}
	return reader, nil
}

func (cs *CollectionSet) scannerFor(c string) (*hfile.Scanner, error) {
	reader, err := cs.readerFor(c)
	if err != nil {
		return nil, err
	}
	s := hfile.NewScanner(reader)
	return &s, nil
}

func (cs *CollectionSet) GetValuesSingle(req *gen.SingleHFileKeyRequest) (r *gen.SingleHFileKeyResponse, err error) {
	log.Printf("[GetValuesSingle] %s (%d keys)\n", *req.HfileName, len(req.SortedKeys))
	reader, err := cs.scannerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

	if req.PerKeyValueLimit != nil {
		log.Println("[GetValuesSingle] PerKeyValueLimit. oh well.")
	}

	if req.CountOnly != nil {
		log.Println("[GetValuesSingle] CountOnly. oh well.")
	}

	res := new(gen.SingleHFileKeyResponse)
	res.Values = make(map[int32][]byte)
	found := int32(0)

	for idx, key := range req.SortedKeys {
		pretty := hex.EncodeToString(key)
		log.Printf("[GetValuesSingle] key: %s\n", pretty)
		value, err, ok := reader.GetFirst(key)
		if err != nil {
			return nil, err
		}
		if ok {
			found++
			res.Values[int32(idx)] = value
		}
	}

	log.Printf("[GetValuesSingle] %s found %d of %d.\n", *req.HfileName, found, len(req.SortedKeys))
	res.KeyCount = &found
	return res, nil
}

func (cs *CollectionSet) GetValuesMulti(req *gen.SingleHFileKeyRequest) (r *gen.MultiHFileKeyResponse, err error) {
	log.Println("[GetValuesMulti]", len(req.SortedKeys))
	reader, err := cs.scannerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

	//out := make(map[int32][]byte)
	res := new(gen.MultiHFileKeyResponse)
	found := int32(0)

	for idx, key := range req.SortedKeys {
		values, err := reader.GetAll(key)
		if err != nil {
			return nil, err
		}
		if len(values) > 0 {
			found += int32(len(values))
			res.Values[int32(idx)] = values
		}
	}

	res.KeyCount = &found
	return res, nil

}

func (cs *CollectionSet) GetValuesForPrefixes(req *gen.PrefixRequest) (r *gen.PrefixResponse, err error) {
	return nil, nil
}

func (cs *CollectionSet) GetValuesMultiSplitKeys(req *gen.MultiHFileSplitKeyRequest) (r *gen.KeyToValuesResponse, err error) {
	return nil, nil
}

func (cs *CollectionSet) GetIterator(req *gen.IteratorRequest) (r *gen.IteratorResponse, err error) {
	return nil, nil
}

func (cs *CollectionSet) GetInfo(req *gen.InfoRequest) (r []*gen.HFileInfo, err error) {
	return nil, nil
}

func (cs *CollectionSet) TestTimeout(waitInMillis int32) (r int32, err error) {
	return 0, nil
}
