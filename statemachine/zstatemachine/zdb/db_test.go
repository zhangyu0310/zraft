package zdb

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func OpenAndPutData(t *testing.T, inputNum uint64, threadNum int, syncMode, memTable bool) []map[uint64]string {
	db, err := OpenDB(nil)
	if err != nil {
		t.Error("Open DB failed, err:", err)
	}
	if threadNum == 0 {
		threadNum = 1
	}
	wg := sync.WaitGroup{}
	idMaps := make([]map[uint64]string, threadNum)

	for i := 0; i < threadNum; i++ {
		begin := inputNum / uint64(threadNum) * uint64(i)
		end := inputNum / uint64(threadNum) * uint64(i+1)
		idMaps[i] = make(map[uint64]string)
		wg.Add(1)
		go func(begin, end uint64, index int) {
			idMap := idMaps[index]
			for i := begin; i < end; i++ {
				id := uuid.NewString()
				value := fmt.Sprintf("TestValue_%d_%s", i, id)
				iStr := strconv.FormatUint(i, 10)
				err := db.Put([]byte("TestKey_"+iStr), []byte(value), &WriteOptions{Sync: syncMode})
				if err != nil {
					t.Error("Put data failed, err:", err)
				}
				if memTable {
					idMap[i] = id
				}
			}
			wg.Done()
		}(begin, end, i)
	}

	wg.Wait()
	maxHeight := db.memTable.list.maxHeight
	t.Log("Memory table max height is", maxHeight)
	err = db.Close()
	if err != nil {
		t.Error("Close db failed, err:", err)
	}
	return idMaps
}

func TestOpenDB(t *testing.T) {
	_, err := OpenDB(nil)
	assert.Nil(t, err, "Open DB failed, err:", err)
}

func TestRecoverDataFromWAL(t *testing.T) {
	inputNum := uint64(1000000)
	threadNum := 100
	syncMode := false
	begin := time.Now()
	idMap := OpenAndPutData(t, inputNum, threadNum, syncMode, true)
	end := time.Now()
	t.Log("Data Size:", inputNum)
	t.Log("Thread Number:", threadNum)
	t.Log("Sync Mode:", syncMode)
	t.Log("Time Used:", end.Sub(begin).Seconds())
	db, err := OpenDB(nil)
	assert.Nil(t, err, "Open DB failed, err:", err)
	for i := uint64(0); i < inputNum; i++ {
		value, err := db.Get([]byte("TestKey_" + strconv.FormatUint(i, 10)))
		assert.Nil(t, err, "Get key failed. Key:", "Test_Key_"+strconv.FormatUint(i, 10))
		strVec := strings.Split(string(value), "_")
		assert.Equal(t, 3, len(strVec), "Value is invalid, value is", string(value))
		assert.Equal(t, "TestValue", strVec[0])
		assert.Equal(t, strconv.FormatUint(i, 10), strVec[1])
		blockId := i / (inputNum / uint64(threadNum))
		assert.Equal(t, idMap[blockId][i], strVec[2])
	}
	err = db.Close()
	assert.Nil(t, err, "Close DB failed, err:", err)
	// Clear
	_ = os.Remove("./ZDB_WAL_MEM")
	_ = os.Remove("./ZDB_WAL_IMM")
}

func TestDBInput(t *testing.T) {
	OpenAndPutData(t, 100000000, 100, false, false)
}
