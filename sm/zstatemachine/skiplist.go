package zstatemachine

import (
	"math/rand"
	"sync/atomic"
)

const (
	MaxHeight = 12
)

type Node struct {
	Key  []byte
	next []atomic.Value
}

type SkipList struct {
	head      *Node
	Compare   func([]byte, []byte) int
	maxHeight atomic.Value
}

func NewSkipList(cmp func([]byte, []byte) int) *SkipList {
	if cmp == nil {
		cmp = func(src1 []byte, src2 []byte) int {
			i := 0
			for {
				if i == len(src1) || i == len(src2) {
					if len(src1) > len(src2) {
						return 1
					} else if len(src1) < len(src2) {
						return -1
					} else {
						return 0
					}
				}
				if src1[i] == src2[i] {
					i++
				} else if src1[i] > src2[i] {
					return 1
				} else {
					return -1
				}
			}
		}
	}
	sl := &SkipList{
		head:      newNode(nil, MaxHeight),
		Compare:   cmp,
		maxHeight: atomic.Value{},
	}
	for i := 0; i < MaxHeight; i++ {
		sl.head.SetNext(i, nil)
	}
	sl.setMaxHeight(1)
	return sl
}

func newNode(key []byte, height int) *Node {
	localKey := make([]byte, len(key))
	copy(localKey, key)
	node := &Node{
		Key:  localKey,
		next: make([]atomic.Value, height),
	}
	return node
}

func (n *Node) Next(height int) *Node {
	return n.next[height].Load().(*Node)
}

func (n *Node) SetNext(height int, next *Node) {
	n.next[height].Store(next)
}

func randomHeight() int {
	var branching uint64
	branching = 4
	height := 1
	for height < MaxHeight && (rand.Uint64()%branching == 0) {
		height++
	}
	return height
}

func (sl *SkipList) GetMaxHeight() int {
	return sl.maxHeight.Load().(int)
}

func (sl *SkipList) setMaxHeight(height int) {
	sl.maxHeight.Store(height)
}

func (sl *SkipList) Insert(key []byte) {
	x, prev := sl.findGreaterOrEqual(key)
	height := randomHeight()
	if height > sl.GetMaxHeight() {
		for i := sl.GetMaxHeight(); i < height; i++ {
			prev[i] = sl.head
		}
		sl.setMaxHeight(height)
	}

	x = newNode(key, height)
	for i := 0; i < height; i++ {
		x.SetNext(i, prev[i].Next(i))
		prev[i].SetNext(i, x)
	}
}

func (sl *SkipList) Contains(key []byte) bool {
	x, _ := sl.findGreaterOrEqual(key)
	if x != nil && (sl.Compare(key, x.Key) == 0) {
		return true
	} else {
		return false
	}
}

func (sl *SkipList) keyIsAfterNode(key []byte, n *Node) bool {
	return n != nil && (sl.Compare(n.Key, key) < 0)
}

func (sl *SkipList) findGreaterOrEqual(key []byte) (*Node, []*Node) {
	x := sl.head
	level := sl.GetMaxHeight() - 1
	prev := make([]*Node, MaxHeight)
	for {
		next := x.Next(level)
		if sl.keyIsAfterNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
			} else {
				level--
			}
		}
	}
}

func (sl *SkipList) findLessThan(key []byte) *Node {
	x := sl.head
	level := sl.GetMaxHeight() - 1
	for {
		next := x.Next(level)
		if next == nil || sl.Compare(next.Key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}

func (sl *SkipList) findLast() *Node {
	x := sl.head
	level := sl.GetMaxHeight() - 1
	for {
		next := x.Next(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}
