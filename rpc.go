package main

import (
	"bytes"
	"encoding/hex"
	"log"

	"github.com/dt/thile/gen"
	"github.com/foursquare/gohfile"
)

type ThriftRpcImpl struct {
	*hfile.CollectionSet
}

func (cs *ThriftRpcImpl) GetValuesSingle(req *gen.SingleHFileKeyRequest) (r *gen.SingleHFileKeyResponse, err error) {
	if Settings.debug {
		log.Printf("[GetValuesSingle] %s (%d keys)\n", *req.HfileName, len(req.SortedKeys))
	}
	reader, err := cs.ScannerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

	if req.PerKeyValueLimit != nil {
		// TODO(davidt) impl
		log.Println("[GetValuesSingle] PerKeyValueLimit. oh well.")
	}

	if req.CountOnly != nil {
		// TODO(davidt) impl
		log.Println("[GetValuesSingle] CountOnly. oh well.")
	}

	res := new(gen.SingleHFileKeyResponse)
	res.Values = make(map[int32][]byte)
	found := int32(0)

	for idx, key := range req.SortedKeys {
		if Settings.debug {
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

	if Settings.debug {
		log.Printf("[GetValuesSingle] %s found %d of %d.\n", *req.HfileName, found, len(req.SortedKeys))
	}
	res.KeyCount = &found
	return res, nil
}

func (cs *ThriftRpcImpl) GetValuesMulti(req *gen.SingleHFileKeyRequest) (r *gen.MultiHFileKeyResponse, err error) {
	if Settings.debug {
		log.Println("[GetValuesMulti]", len(req.SortedKeys))
	}

	reader, err := cs.ScannerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

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

func (cs *ThriftRpcImpl) GetValuesForPrefixes(req *gen.PrefixRequest) (r *gen.PrefixResponse, err error) {
	res := new(gen.PrefixResponse)
	if reader, err := cs.ReaderFor(*req.HfileName); err != nil {
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

func (cs *ThriftRpcImpl) GetValuesMultiSplitKeys(req *gen.MultiHFileSplitKeyRequest) (r *gen.KeyToValuesResponse, err error) {
	res := make(map[string][][]byte)
	scanner, err := cs.ScannerFor(*req.HfileName)
	if err != nil {
		return nil, err
	}

	for _, parts := range RevProduct(req.SplitKey) {
		// TODO(davidt): avoid allocing concated key by adding split-key search lower down.
		key := bytes.Join(parts, nil)

		if values, err := scanner.GetAll(key); err != nil {
			return nil, err
		} else if len(values) > 0 {
			res[string(key)] = values
		}
	}
	return &gen.KeyToValuesResponse{res}, nil
}

func (cs *ThriftRpcImpl) GetIterator(req *gen.IteratorRequest) (r *gen.IteratorResponse, err error) {
	return nil, nil
}

func (cs *ThriftRpcImpl) GetInfo(req *gen.InfoRequest) (r []*gen.HFileInfo, err error) {
	return nil, nil
}

func (cs *ThriftRpcImpl) TestTimeout(waitInMillis int32) (r int32, err error) {
	return 0, nil
}
