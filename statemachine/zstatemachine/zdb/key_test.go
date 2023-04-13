package zdb

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestInternalKey_GetSequenceNum(t *testing.T) {
	key1 := NewInternalKey([]byte("test1"), 0, ValueTypeValue)
	assert.Equal(t, uint64(0), key1.GetSequenceNum())
	key2 := NewInternalKey([]byte("test2"), 123456, ValueTypeValue)
	assert.Equal(t, uint64(123456), key2.GetSequenceNum())
	key3 := NewInternalKey([]byte("test3"), 0, ValueTypeDeletion)
	assert.Equal(t, uint64(0), key3.GetSequenceNum())
	key4 := NewInternalKey([]byte("test4"), 123456, ValueTypeDeletion)
	assert.Equal(t, uint64(123456), key4.GetSequenceNum())
}

func TestInternalKey_IsDeleted(t *testing.T) {
	key1 := NewInternalKey([]byte("test1"), 0, ValueTypeValue)
	assert.False(t, key1.IsDeleted())
	key2 := NewInternalKey([]byte("test2"), 123456, ValueTypeValue)
	assert.False(t, key2.IsDeleted())
	key3 := NewInternalKey([]byte("test3"), 0, ValueTypeDeletion)
	assert.True(t, key3.IsDeleted())
	key4 := NewInternalKey([]byte("test4"), 123456, ValueTypeDeletion)
	assert.True(t, key4.IsDeleted())
}

func TestInternalKeyEncodeDecode(t *testing.T) {
	const TestTimes = 10000000
	uuids := make([]string, 0, TestTimes)
	keys := make([][]byte, 0, TestTimes)
	for i := 0; i < TestTimes; i++ {
		u := uuid.New().String()
		uuids = append(uuids, u)
		valType := uint8(ValueTypeValue)
		r := rand.Intn(10)
		if r > 4 {
			valType = ValueTypeDeletion
		}
		key := NewInternalKey([]byte(u), uint64(i), valType)
		data := InternalKeyEncode(key)
		keys = append(keys, data)
	}
	for i := 0; i < TestTimes; i++ {
		u := uuids[i]
		data := keys[i]
		key := InternalKeyDecode(data)
		assert.Equal(t, uint64(i), key.GetSequenceNum(),
			"i is %d, seq is %d", uint64(i), key.GetSequenceNum())
		assert.Equal(t, u, string(key.GetUserKey()))
	}
}

func TestInternalKeyComparator_Name(t *testing.T) {
	comparator := &InternalKeyComparator{}
	assert.Equal(t, "ZDB.InternalKeyComparator", comparator.Name())
}

func TestInternalKeyComparator_Compare(t *testing.T) {
	// comparator := NewInternalKeyComparator(&StringComparator{})
	// comparator.Compare()
}

func TestInternalKeyComparator_CompareInternalKey(t *testing.T) {

}
