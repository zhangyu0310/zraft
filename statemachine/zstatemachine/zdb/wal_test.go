package zdb

import (
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func initLogWriter(t *testing.T) *LogWriter {
	f, err := os.OpenFile("./test_log",
		os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0666)
	assert.Nil(t, err, "Open test file failed.")
	stat, err := f.Stat()
	assert.Nil(t, err, "Test file stat failed.")
	writer := NewLogWriter(f, stat.Size())
	return writer
}

func initCheckFileHandler(t *testing.T) *os.File {
	checkF, err := os.OpenFile("./test_log",
		os.O_RDONLY, 0666)
	assert.Nil(t, err, "Open check file failed.")
	return checkF
}

func cleanTestEnv() {
	_ = os.Remove("./test_log")
}

func TestLogWriter_AppendRecord_FullLogType(t *testing.T) {
	writer := initLogWriter(t)
	testId := uuid.New().String()
	err := writer.AppendRecord([]byte(testId))
	assert.Nil(t, err, "Append record failed.")
	checkF := initCheckFileHandler(t)
	buffer := make([]byte, LogBlockSize)
	size, err := checkF.Read(buffer)
	assert.Nil(t, err, "Read check file failed.")
	assert.Greater(t, size, 7)
	testRecord := make([]byte, 0, 36)
	for {
		end := false
		switch buffer[6] {
		case FullLogType:
			length := DecodeFixedUint16(GetFixedUint16(buffer, 4))
			testRecord = append(testRecord, buffer[7:length+7]...)
			end = true
		case FirstLogType:
			assert.True(t, false, "This case is not for first log type")
		case MiddleLogType:
			assert.True(t, false, "This case is not for middle log type")
		case LastLogType:
			assert.True(t, false, "This case is not for last log type")
		}
		if end {
			assert.Equal(t, testId, string(testRecord))
			break
		}
	}
	cleanTestEnv()
}

func TestLogWriter_AppendRecord_FLLogType(t *testing.T) {
	writer := initLogWriter(t)
	testId := make([]byte, 0, LogBlockSize+1024)
	for len(testId) < LogBlockSize {
		testId = append(testId, []byte(uuid.New().String())...)
	}
	err := writer.AppendRecord(testId)
	assert.Nil(t, err, "Append record failed.")
	checkF := initCheckFileHandler(t)
	buffer := make([]byte, 0, LogBlockSize)
	testRecord := make([]byte, 0, LogBlockSize+1024)
	for {
		if len(buffer) <= 7 {
			tmpBuffer := make([]byte, LogBlockSize)
			buffer = make([]byte, 0, LogBlockSize)
			size, err := checkF.Read(tmpBuffer)
			assert.Nil(t, err, "Read check file failed.")
			assert.Greater(t, size, 7)
			buffer = append(buffer, tmpBuffer[:size]...)
		}

		end := false
		switch buffer[6] {
		case FullLogType:
			assert.True(t, false, "This case is not for full log type")
		case FirstLogType:
			length := DecodeFixedUint16(GetFixedUint16(buffer, 4))
			testRecord = append(testRecord, buffer[7:length+7]...)
			buffer = buffer[length+7:]
		case MiddleLogType:
			assert.True(t, false, "This case is not for middle log type")
		case LastLogType:
			length := DecodeFixedUint16(GetFixedUint16(buffer, 4))
			testRecord = append(testRecord, buffer[7:length+7]...)
			end = true
		}
		if end {
			if len(testId) != len(testRecord) {
				assert.True(t, false,
					"Data length is different: origin [%d], record [%d]",
					len(testId), len(testRecord))
			}
			count := 0
			for count < len(testId) {
				if testId[count] != testRecord[count] {
					assert.True(t, false, "Data different at index %d", count)
					break
				}
				count++
			}
			break
		}
	}
	cleanTestEnv()
}

func TestLogWriter_AppendRecord_FMLLogType(t *testing.T) {
	writer := initLogWriter(t)
	testId := make([]byte, 0, LogBlockSize*5)
	for len(testId) < LogBlockSize*4 {
		testId = append(testId, []byte(uuid.New().String())...)
	}
	err := writer.AppendRecord(testId)
	assert.Nil(t, err, "Append record failed.")
	checkF := initCheckFileHandler(t)
	buffer := make([]byte, 0, LogBlockSize)
	testRecord := make([]byte, 0, LogBlockSize*5)
	for {
		if len(buffer) <= 7 {
			tmpBuffer := make([]byte, LogBlockSize)
			buffer = make([]byte, 0, LogBlockSize)
			size, err := checkF.Read(tmpBuffer)
			assert.Nil(t, err, "Read check file failed.")
			assert.Greater(t, size, 7)
			buffer = append(buffer, tmpBuffer[:size]...)
		}

		end := false
		switch buffer[6] {
		case FullLogType:
			assert.True(t, false, "This case is not for full log type")
		case FirstLogType:
			length := DecodeFixedUint16(GetFixedUint16(buffer, 4))
			testRecord = append(testRecord, buffer[7:length+7]...)
			buffer = buffer[length+7:]
		case MiddleLogType:
			length := DecodeFixedUint16(GetFixedUint16(buffer, 4))
			testRecord = append(testRecord, buffer[7:length+7]...)
			buffer = buffer[length+7:]
		case LastLogType:
			length := DecodeFixedUint16(GetFixedUint16(buffer, 4))
			testRecord = append(testRecord, buffer[7:length+7]...)
			end = true
		}
		if end {
			if len(testId) != len(testRecord) {
				assert.True(t, false,
					"Data length is different: origin [%d], record [%d]",
					len(testId), len(testRecord))
			}
			count := 0
			for count < len(testId) {
				if testId[count] != testRecord[count] {
					assert.True(t, false, "Data different at index %d", count)
					break
				}
				count++
			}
			break
		}
	}
	cleanTestEnv()
}

func initLogReader(t *testing.T) *LogReader {
	f, err := os.OpenFile("./test_log", os.O_RDONLY, 0666)
	assert.Nil(t, err, "Open test file failed.")
	reader, err := NewLogReader(f, 0)
	assert.Nil(t, err, "New log reader failed.")
	return reader
}

func TestLogReader_ReadRecord(t *testing.T) {
	const TestTimes = 100
	writer := initLogWriter(t)
	testRecordVec := make([][]byte, 0)
	for i := 0; i < TestTimes; i++ {
		repeat := rand.Intn(2000)
		record := make([]byte, 0, LogBlockSize)
		for j := 0; j < repeat; j++ {
			record = append(record, []byte(uuid.New().String())...)
		}
		err := writer.AppendRecord(record)
		assert.Nil(t, err, "Append record failed.")
		testRecordVec = append(testRecordVec, record)
	}
	reader := initLogReader(t)
	for i := 0; i < TestTimes; i++ {
		record, err := reader.ReadRecord()
		assert.Nil(t, err, "Read record failed.")
		assert.Equal(t, testRecordVec[i], record,
			"Record different: origin [%s], record [%s]",
			string(testRecordVec[i]), string(record))
	}
	for i := 0; i < 10; i++ {
		record, err := reader.ReadRecord()
		assert.Equal(t, err, io.EOF)
		assert.Nil(t, record)
	}
	cleanTestEnv()
}
