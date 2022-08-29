package zstatemachine

import (
	"os"

	zlog "github.com/zhangyu0310/zlogger"
)

const (
	ZeroLogType   = 0
	FullLogType   = 1
	FirstLogType  = 2
	MiddleLogType = 3
	LastLogType   = 4
)

const LogBlockSize = 32768

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

type Writer struct {
	file        *os.File
	blockOffset int
}

func NewLogWriter(logName string) (*Writer, error) {
	f, err := os.OpenFile(logName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		zlog.ErrorF("Open file %s failed, err: %s", logName, err)
		return nil, err
	}
	w := &Writer{file: f}
	return w, nil
}

func (w *Writer) AppendRecord(record []byte) {
	recordLen := len(record)
	first := true
	records := make([]*LogRecord, 0, 10)
	tmpRecord := record
	for {
		newBlock := false
		surplus := LogBlockSize - w.blockOffset
		var logRecord *LogRecord
		if recordLen+7 > surplus {
			// This is a first log
			if first {
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(uint16(surplus - 7)),
					recordType: FirstLogType,
					data:       tmpRecord[:surplus-7],
				}
				first = false
			} else {
				// This is a middle log
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(LogBlockSize - 7),
					recordType: MiddleLogType,
					data:       tmpRecord[:surplus-7],
				}
			}
			tmpRecord = tmpRecord[surplus-7:]
			recordLen = recordLen - surplus
			newBlock = true
		} else {
			// This is a full log
			if first {
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(uint16(recordLen)),
					recordType: FullLogType,
					data:       record,
				}
				first = false
			} else { // This is a last log
				logRecord = &LogRecord{
					checksum:   EncodeFixedUint32(0),
					length:     EncodeFixedUint16(uint16(recordLen)),
					recordType: LastLogType,
					data:       record,
				}
			}
			newBlock = false
		}
		records = append(records, logRecord)
		if newBlock {
			w.blockOffset = 0
		} else {
			if LogBlockSize-w.blockOffset < 7 {
				for i := 0; i < LogBlockSize-w.blockOffset; i++ {
					records[len(records)-1].data = append(records[len(records)-1].data, 0)
				}
				w.blockOffset = 0
			}
			break
		}
	}
	// for _, r := range records {
	// 	data := make([]byte, 0, 10)
	// 	data = append(data, r.checksum)
	// }
}
