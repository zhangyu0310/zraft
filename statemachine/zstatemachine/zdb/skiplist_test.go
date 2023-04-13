package zdb

import (
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func slCmpFunc(src1 []byte, src2 []byte) int {
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

func TestNewSkipList(t *testing.T) {
	sl := NewSkipList(slCmpFunc)
	assert.Equal(t, 1, sl.GetMaxHeight())
	assert.Equal(t, MaxHeight, len(sl.head.next))
}

func TestSkipList_Insert(t *testing.T) {
	sl := NewSkipList(slCmpFunc)
	idMap := make(map[string]int)
	loopRound := 100
	for i := 0; i < loopRound; i++ {
		id := uuid.New().String()
		id = strconv.Itoa(i) + ":" + id
		sl.Insert([]byte(id))
		idMap[id] = 0
	}
	node := sl.head
	for i := 0; i < loopRound; i++ {
		node = node.Next(0)
		idMap[string(node.Key)]++
	}
	for _, count := range idMap {
		assert.Equal(t, 1, count)
	}
}

func TestSkipList_Contains(t *testing.T) {
	sl := NewSkipList(slCmpFunc)
	idMap := make(map[int]string)
	loopRound := 100
	for i := 0; i < loopRound; i++ {
		id := uuid.New().String()
		sl.Insert([]byte(id))
		idMap[i] = id
	}
	for i := 0; i < loopRound; i++ {
		id := idMap[i]
		assert.True(t, sl.Contains([]byte(id)))
	}
}
