package entry

import "sync"

const (
	OpInvalid  = -1
	OpPutEntry = 0
	OpDelEntry = 1
	OpGetEntry = 2
)

func GetOpStr(op int8) string {
	switch op {
	case OpInvalid:
		return "Invalid"
	case OpPutEntry:
		return "PutEntry"
	case OpDelEntry:
		return "DelEntry"
	case OpGetEntry:
		return "GetEntry"
	}
	return "Unknown"
}

type Entry struct {
	Term     uint64
	Index    uint64
	Op       int8
	Key      []byte
	Value    []byte
	Callback func([]byte)
}

type BlockingQueue struct {
	sync.Mutex
	cond    *sync.Cond
	entries []*Entry
}

func NewBlockingQueue() (queue *BlockingQueue) {
	queue = &BlockingQueue{}
	queue.cond = sync.NewCond(queue)
	queue.entries = make([]*Entry, 0, 1000)
	return
}

func (q *BlockingQueue) Push(entry *Entry) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.entries = append(q.entries, entry)
	q.cond.Signal()
}

func (q *BlockingQueue) Pop() *Entry {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.cond.Wait()
	entry := q.entries[0]
	q.entries = q.entries[1:]
	return entry
}

func (q *BlockingQueue) PopAll() []*Entry {
	q.cond.L.Lock()
	for len(q.entries) == 0 {
		q.cond.Wait()
	}
	entries := q.entries
	q.entries = make([]*Entry, 0, 1000)
	q.cond.L.Unlock()
	return entries
}

type UnblockingQueue struct {
	sync.Mutex
	entries []*Entry
}

func NewUnblockingQueue() (queue *UnblockingQueue) {
	queue = &UnblockingQueue{}
	queue.entries = make([]*Entry, 0, 1000)
	return
}

func (q *UnblockingQueue) Push(entry *Entry) {
	q.Lock()
	defer q.Unlock()
	q.entries = append(q.entries, entry)
}

func (q *UnblockingQueue) PopAll() []*Entry {
	q.Lock()
	defer q.Unlock()
	if len(q.entries) == 0 {
		return nil
	}
	entries := q.entries
	q.entries = make([]*Entry, 0, 1000)
	return entries
}
