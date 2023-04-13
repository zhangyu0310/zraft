package zdb

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func BlockBuilderWR(t *testing.T, insertTimes, restartCount int) {
	builder := NewBlockBuilder()
	for i := 0; i < insertTimes; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := uuid.NewString()
		builder.Append([]byte(key), []byte(value))
	}
	// Build block
	data, last, err := builder.Build(restartCount)
	assert.Nil(t, err)

	// Put block data into block struct
	block, err := NewBlock(data, &StringComparator{})
	assert.Nil(t, err)
	blockIter := NewBlockIter(block)
	blockIter.SeekToFirst()
	count1 := 0
	for blockIter.Valid() {
		realKey, _, err := blockIter.Get()
		assert.Nil(t, err)
		expectedKey := fmt.Sprintf("Key-%d", count1)
		count1++
		assert.Equal(t, expectedKey, string(realKey))
		if count1 == insertTimes {
			assert.Equal(t, expectedKey, string(last))
			break
		}
		err = blockIter.Next()
		assert.Nil(t, err)
	}
}

func TestBlockBuilder_Build(t *testing.T) {
	BlockBuilderWR(t, 100000, MaxRestartCount)
	BlockBuilderWR(t, 100000, 1)
	BlockBuilderWR(t, 1000, 1000)
}
