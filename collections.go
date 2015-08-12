package main

import (
	"fmt"
	"log"
	"os"

	"github.com/dt/thile/gen"
)

type Collection struct {
	Name string
	Path string
}

type CollectionSet struct {
	readers map[string]*HfileReader
}

func LoadCollections(collections []Collection) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.readers = make(map[string]*HfileReader)

	for _, config := range collections {

		f, err := os.OpenFile(config.Path, os.O_RDONLY, 0)

		if err != nil {
			return nil, err
		}

		reader, err := NewHfileReader(f)
		if err != nil {
			return nil, err
		}

		cs.readers[config.Name] = &reader
	}

	return cs, nil
}

func (cs *CollectionSet) readerFor(c string) (*HfileReader, error) {
	reader, ok := cs.readers[c]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", c)
	}
	return reader, nil
}

func (cs *CollectionSet) GetValuesSingle(req *gen.SingleHFileKeyRequest) (r *gen.SingleHFileKeyResponse, err error) {
	log.Println("[GetValuesSingle]", len(req.SortedKeys))
	reader, err := cs.readerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

	//out := make(map[int32][]byte)
	res := new(gen.SingleHFileKeyResponse)
	found := int32(0)

	for idx, key := range req.SortedKeys {
		value, ok := reader.GetFirst(key)
		if ok {
			found++
			res.Values[int32(idx)] = value
		}
	}

	res.KeyCount = &found
	return res, nil
}

func (cs *CollectionSet) GetValuesMulti(req *gen.SingleHFileKeyRequest) (r *gen.MultiHFileKeyResponse, err error) {
	log.Println("[GetValuesMulti]", len(req.SortedKeys))
	reader, err := cs.readerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

	//out := make(map[int32][]byte)
	res := new(gen.MultiHFileKeyResponse)
	found := int32(0)

	for idx, key := range req.SortedKeys {
		values := reader.GetAll(key)
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
