package zdb

import (
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMemTable_Get(t *testing.T) {
	memTable := NewMemTable(&StringComparator{})
	lookupKey := MakeLookupKey([]byte("Test"), 0)
	exist, _, err := memTable.Get(lookupKey)
	assert.Nil(t, err)
	assert.False(t, exist)
}

func TestMemTable_Add(t *testing.T) {
	testTimes := uint64(1000000)
	memTable := NewMemTable(&StringComparator{})
	testMap := make(map[uint64]string)
	for i := uint64(0); i < testTimes; i++ {
		key := "Test_" + strconv.Itoa(int(i))
		val := uuid.New().String()
		testMap[i] = val
		memTable.Add(i, ValueTypeValue, []byte(key), []byte(val))
	}
	for i := uint64(0); i < testTimes; i++ {
		key := "Test_" + strconv.Itoa(int(i))
		val := testMap[i]
		lookupKey := MakeLookupKey([]byte(key), i)
		exist, memVal, err := memTable.Get(lookupKey)
		assert.Nil(t, err)
		assert.True(t, exist)
		assert.Equal(t, val, string(memVal))
	}
}
