package hfile

var IndexMagic = []byte("IDXBLK)+")
var DataMagic = []byte("DATABLK*")
var TrailerMagic = []byte("TRABLK\"$")

var CompressionNone = uint32(2)
var CompressionSnappy = uint32(3)
