package util

func RevProduct(l [][][]byte) [][][]byte {
	if len(l) == 0 {
		return [][][]byte{nil}
	}

	head := l[0]
	p := RevProduct(l[1:])
	ret := make([][][]byte, 0, len(head)*len(p))
	for _, i := range head {
		for _, r := range p {
			ret = append(ret, append([][]byte{i}, r...))
		}
	}
	return ret
}
