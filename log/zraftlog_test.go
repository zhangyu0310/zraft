package log

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZRaftLog_Store(t *testing.T) {
	log := NewZRaftLog()
	err := log.Init()
	assert.Nil(t, err)
	err = log.Store(&Entry{
		Op:    OpPutEntry,
		Term:  1,
		Index: 1,
		Key:   []byte("test1"),
		Value: []byte("Test1"),
	})
	assert.Nil(t, err)

	entry, err := log.Load(1)
	assert.Nil(t, err)
	assert.Equal(t, int8(OpPutEntry), entry.Op)
	assert.Equal(t, uint64(1), entry.Term)
	assert.Equal(t, uint64(1), entry.Index)
	assert.Equal(t, "test1", string(entry.Key))
	assert.Equal(t, "Test1", string(entry.Value))

	_ = os.Remove("./zraft_log")
}

func TestZRaftLog(t *testing.T) {
	for i := 1; i <= 100; i++ {
		log := NewZRaftLog()
		assert.Nil(t, log.Init())
		assert.Nil(t, log.Store(&Entry{
			Term:  1,
			Index: 0,
			Op:    OpPutEntry,
			Key:   nil,
			Value: nil,
		}))
		for j := 1; j <= 100; j++ {
			key := fmt.Sprintf("test key %d", j)
			value := fmt.Sprintf("test value %d", j)
			err := log.Store(&Entry{
				Op:    OpPutEntry,
				Term:  uint64(i),
				Index: uint64(j),
				Key:   []byte(key),
				Value: []byte(value),
			})
			assert.Nil(t, err)
		}
		assert.Nil(t, log.Destroy())
		log = NewZRaftLog()
		assert.Nil(t, log.Init())
		for j := 1; j <= 100; j++ {
			key := fmt.Sprintf("test key %d", j)
			value := fmt.Sprintf("test value %d", j)
			entry, err := log.Load(uint64(j))
			assert.Nil(t, err)
			assert.Equal(t, int8(OpPutEntry), entry.Op)
			assert.Equal(t, uint64(i), entry.Term)
			assert.Equal(t, uint64(j), entry.Index)
			assert.Equal(t, key, string(entry.Key))
			assert.Equal(t, value, string(entry.Value))
		}
		_ = os.Remove("./zraft_log")
	}
}

func TestEncodeAndDecodeVarInt64(t *testing.T) {
	vNum1 := EncodeVarUint64(127)
	assert.Equal(t, uint64(127), DecodeVarUint64(vNum1))
	vNum2 := EncodeVarUint64(128)
	assert.Equal(t, uint64(128), DecodeVarUint64(vNum2))
	vNum3 := EncodeVarUint64(123456)
	assert.Equal(t, uint64(123456), DecodeVarUint64(vNum3))
	vNum4 := EncodeVarUint64(99999999)
	assert.Equal(t, uint64(99999999), DecodeVarUint64(vNum4))

	for i := 0; i < 1000000; i++ {
		rNum := rand.Int63()
		vNum := EncodeVarUint64(uint64(rNum))
		assert.Equal(t, uint64(rNum), DecodeVarUint64(vNum))
	}
}
