package util

import "bytes"

type Keys [][]byte

func (s Keys) Len() int {
	return len(s)
}
func (s Keys) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}
func (s Keys) Swap(i, j int) {
	m := s[j]
	s[j] = s[i]
	s[i] = m
}
