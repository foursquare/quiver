package main

import (
	"log"

	"github.com/dt/thile/gen"
)

type HfileImpl struct {
}

func (h *HfileImpl) GetValuesSingle(req *gen.SingleHFileKeyRequest) (r *gen.SingleHFileKeyResponse, err error) {
	log.Println("[GetValuesSingle]", len(req.SortedKeys))
	return nil, nil
}

func (h *HfileImpl) GetValuesMulti(req *gen.SingleHFileKeyRequest) (r *gen.MultiHFileKeyResponse, err error) {
	return nil, nil
}

func (h *HfileImpl) GetValuesForPrefixes(req *gen.PrefixRequest) (r *gen.PrefixResponse, err error) {
	return nil, nil
}

func (h *HfileImpl) GetValuesMultiSplitKeys(req *gen.MultiHFileSplitKeyRequest) (r *gen.KeyToValuesResponse, err error) {
	return nil, nil
}

func (h *HfileImpl) GetIterator(req *gen.IteratorRequest) (r *gen.IteratorResponse, err error) {
	return nil, nil
}

func (h *HfileImpl) GetInfo(req *gen.InfoRequest) (r []*gen.HFileInfo, err error) {
	return nil, nil
}

func (h *HfileImpl) TestTimeout(waitInMillis int32) (r int32, err error) {
	return 0, nil
}
