package zdb

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMemTable_Get(t *testing.T) {
	memTable := NewMemTable(&StringComparator{})
	lookupKey := MakeLookupKey([]byte("Test"), 0)
	exist, _, _, err := memTable.Get(lookupKey)
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
		lookupKey := MakeLookupKey([]byte(key), testTimes+1)
		exist, _, memVal, err := memTable.Get(lookupKey)
		assert.Nil(t, err)
		assert.True(t, exist)
		assert.Equal(t, val, string(memVal))
	}
}

func TestMemTable_AddSameKey(t *testing.T) {
	testTimes := uint64(1000000)
	memTable := NewMemTable(&StringComparator{})
	key := "Test_Key_Same"
	for i := uint64(0); i < testTimes; i++ {
		value := fmt.Sprintf("Test_Value_%d", i)
		memTable.Add(i, ValueTypeValue, []byte(key), []byte(value))
	}
	lookup := MakeLookupKey([]byte(key), MaxSequenceNum)
	exist, _, result, err := memTable.Get(lookup)
	assert.Nil(t, err)
	assert.True(t, exist)
	assert.Equal(t, fmt.Sprintf("Test_Value_%d", testTimes-1), string(result))
}

type record struct {
	Key      []byte
	Value    []byte
	Sequence uint64
	Type     uint8
}

func TestMemTableIterator_Get(t *testing.T) {
	testTimes := uint64(100)
	memTable := NewMemTable(&StringComparator{})

	var lastKey string
	recordVec := make([]*record, 0, testTimes)
	for i := uint64(0); i < testTimes; i++ {
		key := fmt.Sprintf("Test_Key_%d", i)
		value := fmt.Sprintf("Test_Value_%d", i)
		r := rand.Intn(10)
		if r > 4 {
			key = lastKey
		}
		lastKey = key
		valueType := ValueTypeValue
		r = rand.Intn(10)
		if r > 4 {
			valueType = ValueTypeDeletion
		}
		record := &record{
			Key:      []byte(key),
			Value:    []byte(value),
			Sequence: i,
			Type:     uint8(valueType),
		}
		recordVec = append(recordVec, record)
		memTable.Add(i, uint8(valueType), []byte(key), []byte(value))
	}

	iter := NewMemTableIterator(memTable)
	iter.SeekToFirst()
	for iter.Valid() {
		internalKey, value := iter.Get()
		key := InternalKeyDecode(internalKey)
		t.Log("User Key:", string(key.GetUserKey()))
		t.Log("User Value:", string(value))
		t.Log("Sequence Number:", key.GetSequenceNum())
		t.Log("Key Type:", key.IsDeleted())
		iter.Next()
	}
}

func TestKeyInsertOrder(t *testing.T) {
	memTable := NewMemTable(&StringComparator{})
	memTable.Add(1, ValueTypeValue, []byte("a"), []byte("a_1"))
	memTable.Add(2, ValueTypeValue, []byte("b"), []byte("b_2"))
	memTable.Add(3, ValueTypeValue, []byte("c"), []byte("c_3"))
	memTable.Add(4, ValueTypeValue, []byte("a"), []byte("a_4"))
	memTable.Add(5, ValueTypeValue, []byte("b"), []byte("b_5"))
	memTable.Add(6, ValueTypeValue, []byte("c"), []byte("c_6"))
	memTable.Add(7, ValueTypeValue, []byte("a"), []byte("a_7"))
	memTable.Add(8, ValueTypeValue, []byte("b"), []byte("b_8"))
	memTable.Add(9, ValueTypeValue, []byte("c"), []byte("c_9"))
	iter := NewMemTableIterator(memTable)
	iter.SeekToFirst()
	for iter.Valid() {
		key, value := iter.Get()
		internalKey := InternalKeyDecode(key)
		t.Log("key:", string(internalKey.GetUserKey()), "seq:", internalKey.GetSequenceNum())
		t.Log("value:", string(value))
		iter.Next()
	}
}
