package hfile

import (
	"bytes"
	"fmt"
)

type OrderedOps struct {
	lastKey []byte
}

func (s *OrderedOps) ResetState() {
	s.lastKey = nil
}

func (s *OrderedOps) Same(key []byte) bool {
	return s.lastKey != nil && bytes.Equal(s.lastKey, key)
}

func (s *OrderedOps) CheckIfKeyOutOfOrder(key []byte) error {
	if s.lastKey != nil && bytes.Compare(s.lastKey, key) > 0 {
		return fmt.Errorf("Keys out of order! %v > %v", s.lastKey, key)
	}
	s.lastKey = key
	return nil
}
