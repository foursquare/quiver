package main

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/dt/thile/gen"
	"github.com/foursquare/gohfile"
)

type Collection struct {
	cfg    *hfile.CollectionConfig
	reader *hfile.Reader
}

type CollectionSet struct {
	settings    *Settings
	collections map[string]Collection
}

func LoadCollections(settings *Settings, collections []hfile.CollectionConfig) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.settings = settings
	cs.collections = make(map[string]Collection)

	for _, cfg := range collections {
		reader, err := hfile.NewReaderFromConfig(&cfg, settings.debug)
		if err != nil {
			return nil, err
		}

		cs.collections[cfg.Name] = Collection{&cfg, reader}
	}

	return cs, nil
}

func (cs *CollectionSet) readerFor(name string) (*hfile.Reader, error) {
	c, ok := cs.collections[name]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", name)
	}
	return c.reader, nil
}

func (cs *CollectionSet) scannerFor(c string) (*hfile.Scanner, error) {
	reader, err := cs.readerFor(c)
	if err != nil {
		return nil, err
	}
	s := hfile.NewScanner(reader)
	return s, nil
}

func (cs *CollectionSet) GetValuesSingle(req *gen.SingleHFileKeyRequest) (r *gen.SingleHFileKeyResponse, err error) {
	if cs.settings.debug {
		log.Printf("[GetValuesSingle] %s (%d keys)\n", *req.HfileName, len(req.SortedKeys))
	}
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
		if cs.settings.debug {
			log.Printf("[GetValuesSingle] key: %s\n", hex.EncodeToString(key))
		}
		value, err, ok := reader.GetFirst(key)
		if err != nil {
			return nil, err
		}
		if ok {
			found++
			res.Values[int32(idx)] = value
		}
	}

	if cs.settings.debug {
		log.Printf("[GetValuesSingle] %s found %d of %d.\n", *req.HfileName, found, len(req.SortedKeys))
	}
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
	res := new(gen.PrefixResponse)
	if reader, err := cs.readerFor(*req.HfileName); err != nil {
		return nil, err
	} else {
		i := reader.NewIterator()
		if res.Values, err = i.AllForPrfixes(req.SortedKeys); err != nil {
			return nil, err
		} else {
			return res, nil
		}
	}
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
