package log

import (
	"io/ioutil"
	"os"

	zlog "github.com/zhangyu0310/zlogger"
)

type ZRaftLog struct {
	log  map[uint64]*Entry
	file *os.File
}

// type ZRaftLogOnDisk struct {
// 	Op        byte
// 	Term      VarUint64
// 	Index     VarUint64
// 	KeySize   VarUint64
// 	Key       []byte
// 	ValueSize VarUint64
// 	Value     []byte
// }

func NewZRaftLog() *ZRaftLog {
	log := &ZRaftLog{
		log:  make(map[uint64]*Entry),
		file: nil,
	}
	return log
}

func (l *ZRaftLog) Init(_ ...interface{}) error {
	// TODO: config file path
	var err error
	l.file, err = os.OpenFile("./zraft_log",
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	err = l.LoadAllLogsFromDisk()
	return err
}

func (l *ZRaftLog) Destroy(_ ...interface{}) error {
	_ = l.file.Sync()
	return l.file.Close()
}

func (l *ZRaftLog) Store(entry *Entry) error {
	l.log[entry.Index] = entry
	return l.AppendLog(entry)
}

func (l *ZRaftLog) Load(index uint64) (*Entry, error) {
	return l.log[index], nil
}

func (l *ZRaftLog) LoadAllLogsFromDisk() error {
	data, err := ioutil.ReadAll(l.file)
	if err != nil {
		return err
	}
	// state machine to decode log
	for i := 0; i < len(data); {
		entry := &Entry{}
		var tmpVal VarUint64
		// Get Op
		entry.Op = int8(data[i])
		i++
		// Get term
		tmpVal, i = GetVarUint64(data, i)
		entry.Term = DecodeVarUint64(tmpVal)
		// Get index
		tmpVal, i = GetVarUint64(data, i)
		entry.Index = DecodeVarUint64(tmpVal)
		// Get KeySize
		tmpVal, i = GetVarUint64(data, i)
		keySize := DecodeVarUint64(tmpVal)
		// Get Key
		key := make([]byte, keySize)
		copy(key, data[i:i+int(keySize)])
		entry.Key = key
		i += int(keySize)
		// Get ValueSize
		tmpVal, i = GetVarUint64(data, i)
		valueSize := DecodeVarUint64(tmpVal)
		// Get Value
		value := make([]byte, valueSize)
		copy(value, data[i:i+int(valueSize)])
		entry.Value = value
		i += int(valueSize)
		// Apply to log in memory
		switch entry.Op {
		case OpPutEntry:
			l.log[entry.Index] = entry
		case OpDelEntry:
			delete(l.log, entry.Index)
		default:
			zlog.Panic("Entry op is", GetOpStr(entry.Op),
				"I don't know what happened...")
		}
	}
	return err
}

func (l *ZRaftLog) AppendLog(entry *Entry) error {
	data := make([]byte, 0, 64)
	data = append(data, byte(entry.Op))
	data = append(data, EncodeVarUint64(entry.Term)...)
	data = append(data, EncodeVarUint64(entry.Index)...)
	data = append(data, EncodeVarUint64(uint64(len(entry.Key)))...)
	data = append(data, entry.Key...)
	data = append(data, EncodeVarUint64(uint64(len(entry.Value)))...)
	data = append(data, entry.Value...)
	dataLen := len(data)
	dataIndex := 0
	for {
		num, err := l.file.Write(data)
		if err != nil {
			return err
		}
		dataLen -= num
		if dataLen == 0 {
			break
		}
		data = data[dataIndex+num:]
		dataIndex += num
	}
	return nil
}

type VarUint64 []byte

func EncodeVarUint64(n uint64) VarUint64 {
	res := make([]byte, 0, 1)
	for n >= 128 {
		res = append(res, byte(n|128))
		n >>= 7
	}
	res = append(res, byte(n))
	return res
}

func DecodeVarUint64(n VarUint64) uint64 {
	var res uint64
	index := 0
	for shift := 0; shift <= 63; shift += 7 {
		b := n[index]
		index++
		if (b & 128) != 0 {
			res |= uint64(b) & 127 << shift
		} else {
			res |= uint64(b) << shift
			break
		}
	}
	return res
}

func GetVarUint64(data []byte, index int) (VarUint64, int) {
	varUint64 := make(VarUint64, 0, 4)
	for {
		varUint64 = append(varUint64, data[index])
		if data[index]&128 == 0 {
			index++
			break
		}
		index++
	}
	return varUint64, index
}
