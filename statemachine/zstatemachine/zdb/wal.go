package zdb

import (
	"errors"
	"fmt"
	"os"

	zlog "github.com/zhangyu0310/zlogger"
)

const (
	FullLogType   = 1
	FirstLogType  = 2
	MiddleLogType = 3
	LastLogType   = 4
)

const (
	LogBlockSize   = 32768
	LogHeaderSize  = 4 + 2 + 1
	MaxWALFileSize = 128 * 1024 * 1024
)

func getRecordType(data []byte) int {
	return int(data[6])
}

var (
	ErrIncompleteLogFile = errors.New("incomplete log file")
	ErrUnknownLogType    = errors.New("unknown log type")
)

type LogRecord struct {
	checksum   FixedUint32
	length     FixedUint16
	recordType uint8
	data       []byte
}

type LogBlock struct {
	Records []*LogRecord
	padding []byte
}

type LogWriter struct {
	file        *os.File
	blockOffset int
}

type LogReader struct {
	file       *os.File
	readOffset int64
	buffer     []byte
}

// NewLogWriter write log to targetFile.
//   targetFile must have length == fileLength & offset of file must at end.
func NewLogWriter(targetFile *os.File, fileLength int64) *LogWriter {
	return &LogWriter{
		file:        targetFile,
		blockOffset: int(fileLength % LogBlockSize),
	}
}

func UpdateWALFile(old *LogWriter, options *Options) (*LogWriter, error) {
	err := old.Close()
	if err != nil {
		zlog.Error("Update WAL failed, old WAL close failed, err:", err)
		return nil, err
	}
	memWalPath := fmt.Sprintf("%s/%s", options.DataDirPath, NameOfMemWAL)
	immWalPath := fmt.Sprintf("%s/%s", options.DataDirPath, NameOfImmWAL)
	err = os.Rename(memWalPath, immWalPath)
	if err != nil {
		zlog.Error("Update WAL failed, rename old WAL failed, err:", err)
		return nil, err
	}
	file, err := os.OpenFile(memWalPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_EXCL, 0666)
	if err != nil {
		zlog.Error("Update WAL failed, create new memory WAL failed, err:", err)
		return nil, err
	}
	return NewLogWriter(file, 0), nil
}

func (w *LogWriter) AppendRecord(record []byte) error {
	recordLen := len(record)
	first := true
	records := make([]*LogRecord, 0, 10)
	tmpRecord := record
	for {
		newBlock := false
		surplus := LogBlockSize - w.blockOffset
		var logRecord *LogRecord
		if recordLen+LogHeaderSize > surplus {
			// This is a first log
			if first {
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(uint16(surplus - LogHeaderSize)),
					recordType: FirstLogType,
					data:       tmpRecord[:surplus-LogHeaderSize],
				}
				first = false
			} else {
				// This is a middle log
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(LogBlockSize - LogHeaderSize),
					recordType: MiddleLogType,
					data:       tmpRecord[:surplus-LogHeaderSize],
				}
			}
			tmpRecord = tmpRecord[surplus-LogHeaderSize:]
			recordLen = recordLen - surplus + LogHeaderSize
			newBlock = true
		} else {
			// This is a full log
			if first {
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(uint16(recordLen)),
					recordType: FullLogType,
					data:       tmpRecord,
				}
				first = false
			} else { // This is a last log
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(uint16(recordLen)),
					recordType: LastLogType,
					data:       tmpRecord,
				}
			}
			w.blockOffset += recordLen + LogHeaderSize
			newBlock = false
		}
		records = append(records, logRecord)
		if newBlock {
			w.blockOffset = 0
		} else {
			if LogBlockSize-w.blockOffset < LogHeaderSize {
				for i := 0; i < LogBlockSize-w.blockOffset; i++ {
					records[len(records)-1].data = append(records[len(records)-1].data, 0)
				}
				w.blockOffset = 0
			}
			break
		}
	}
	data := make([]byte, 0, 64)
	for _, r := range records {
		data = append(data, r.checksum[:]...)
		data = append(data, r.length[:]...)
		data = append(data, r.recordType)
		data = append(data, r.data...)
	}
	_, err := w.file.Write(data)
	if err != nil {
		zlog.Error("Write WAL failed, err:", err)
		return err
	}
	return nil
}

func (w *LogWriter) Size() (int64, error) {
	stat, err := w.file.Stat()
	return stat.Size(), err
}

func (w *LogWriter) Sync() error {
	return w.file.Sync()
}

func (w *LogWriter) Close() error {
	return w.file.Close()
}

func NewLogReader(targetFile *os.File, offset int64) (*LogReader, error) {
	_, err := targetFile.Seek(offset, 0)
	if err != nil {
		zlog.Error("New log reader seek file failed, err:", err)
		return nil, err
	}
	return &LogReader{
		file:       targetFile,
		readOffset: offset,
		buffer:     make([]byte, 0, LogBlockSize),
	}, nil
}

func (r *LogReader) ReadRecord() ([]byte, error) {
	record := make([]byte, 0, 1024)
	for {
		if len(r.buffer) <= LogHeaderSize {
			r.buffer = make([]byte, 0, LogBlockSize)
			block := make([]byte, LogBlockSize)
			size, err := r.file.Read(block)
			if err != nil {
				return nil, err
			}
			if len(block) < LogHeaderSize {
				return nil, ErrIncompleteLogFile
			}
			r.buffer = append(r.buffer, block[:size]...)
			r.readOffset += int64(size)
		}

		switch getRecordType(r.buffer) {
		case FullLogType:
			length := DecodeFixedUint16(GetFixedUint16(r.buffer, 4))
			record = append(record, r.buffer[LogHeaderSize:length+LogHeaderSize]...)
			r.buffer = r.buffer[length+LogHeaderSize:]
			return record, nil
		case FirstLogType:
			length := DecodeFixedUint16(GetFixedUint16(r.buffer, 4))
			record = append(record, r.buffer[LogHeaderSize:length+LogHeaderSize]...)
			r.buffer = make([]byte, 0, LogBlockSize)
			continue
		case MiddleLogType:
			length := DecodeFixedUint16(GetFixedUint16(r.buffer, 4))
			record = append(record, r.buffer[LogHeaderSize:length+LogHeaderSize]...)
			r.buffer = make([]byte, 0, LogBlockSize)
			continue
		case LastLogType:
			length := DecodeFixedUint16(GetFixedUint16(r.buffer, 4))
			record = append(record, r.buffer[LogHeaderSize:length+LogHeaderSize]...)
			r.buffer = r.buffer[length+LogHeaderSize:]
			return record, nil
		default:
			return nil, ErrUnknownLogType
		}
	}
}

func (r *LogReader) Close() error {
	return r.file.Close()
}
