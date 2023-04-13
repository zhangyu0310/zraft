package zdb

import (
	"math/rand"
	"sync/atomic"

	zlog "github.com/zhangyu0310/zlogger"
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

type SkipListIterator struct {
	skipList *SkipList
	node     *Node
}

func NewSkipList(cmp func([]byte, []byte) int) *SkipList {
	if cmp == nil {
		zlog.Panic("Need a compare function for skip list!")
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

func NewSkipListIterator(sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		skipList: sl,
		node:     nil,
	}
}

func (it *SkipListIterator) Valid() bool {
	return it.node != nil
}

func (it *SkipListIterator) Key() []byte {
	return it.node.Key
}

func (it *SkipListIterator) Next() {
	it.node = it.node.Next(0)
}

func (it *SkipListIterator) Prev() {
	it.node = it.skipList.findLessThan(it.node.Key)
	if it.node == it.skipList.head {
		it.node = nil
	}
}

func (it *SkipListIterator) Seek(key []byte) {
	it.node, _ = it.skipList.findGreaterOrEqual(key)
}

func (it *SkipListIterator) SeekToFirst() {
	it.node = it.skipList.head.Next(0)
}

func (it *SkipListIterator) SeekToLast() {
	it.node = it.skipList.findLast()
	if it.node == it.skipList.head {
		it.node = nil
	}
}
