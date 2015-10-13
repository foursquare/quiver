package lru

import "sync"

type node struct {
	key   int
	block []byte
	older *node
	newer *node
}

type LRU struct {
	size   int
	blocks map[int]*node
	newest *node
	oldest *node

	sync.Mutex
}

func NewLRU(size int) *LRU {
	return &LRU{size, make(map[int]*node), nil, nil, sync.Mutex{}}
}

func (l *LRU) Get(i int) ([]byte, bool) {
	l.Lock()
	defer l.Unlock()
	if b, ok := l.blocks[i]; ok {
		l.moveToFront(b)
		return b.block, true
	}
	return nil, false
}

func (l *LRU) Add(i int, v []byte) {
	l.Lock()
	defer l.Unlock()

	for len(l.blocks) >= l.size {
		delete(l.blocks, l.oldest.key)
		l.oldest = l.oldest.newer
		if l.oldest != nil {
			l.oldest.older = nil
		}
	}

	n := &node{i, v, nil, nil}
	l.moveToFront(n)
	l.blocks[i] = n
}

func (l *LRU) moveToFront(n *node) {
	if n == l.newest {
		return
	}

	if n == l.oldest {
		l.oldest = n.newer
	}

	if n.older != nil {
		n.older.newer = n.newer
	}
	if n.newer != nil {
		n.newer.older = n.older
	}
	n.newer = nil
	n.older = l.newest
	l.newest = n
	if l.oldest == nil {
		l.oldest = n
	}
}
