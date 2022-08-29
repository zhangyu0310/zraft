package log

import "strconv"

const (
	OpPutEntry = 0
	OpDelEntry = 1
	OpGetEntry = 2
	OpInvalid  = 3
)

func GetOpStr(op int8) string {
	switch op {
	case OpPutEntry:
		return "PutEntry"
	case OpDelEntry:
		return "DelEntry"
	case OpGetEntry:
		return "GetEntry"
	case OpInvalid:
		return "Invalid"
	}
	return "Unknown: " + strconv.Itoa(int(op))
}

type Entry struct {
	Op    int8
	Term  uint64
	Index uint64
	Key   []byte
	Value []byte
}

// Log store raft log(Entry) persistently
type Log interface {
	Init(...interface{}) error
	Destroy(...interface{}) error
	// Store Entry.Index is unique key of log
	Store(entry *Entry) error
	// Load If entry is not exist, return Entry = nil instead of error
	Load(index uint64) (*Entry, error)
}
