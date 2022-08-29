package queue

import (
	"sync"

	"zraft/log"
)

type Wrapper struct {
	Entry    *log.Entry
	Callback func(error)
}

type BlockingQueue struct {
	sync.Mutex
	cond    *sync.Cond
	entries []*log.Entry
}

func NewBlockingQueue() (queue *BlockingQueue) {
	queue = &BlockingQueue{}
	queue.cond = sync.NewCond(queue)
	queue.entries = make([]*log.Entry, 0, 1000)
	return
}

func (q *BlockingQueue) Push(entry *log.Entry) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.entries = append(q.entries, entry)
	q.cond.Signal()
}

func (q *BlockingQueue) PushAll(entries []*log.Entry) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.entries = append(q.entries, entries...)
	q.cond.Signal()
}

func (q *BlockingQueue) Pop() *log.Entry {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.cond.Wait()
	entry := q.entries[0]
	q.entries = q.entries[1:]
	return entry
}

func (q *BlockingQueue) PopAll() []*log.Entry {
	q.cond.L.Lock()
	for len(q.entries) == 0 {
		q.cond.Wait()
	}
	entries := q.entries
	q.entries = make([]*log.Entry, 0, 1000)
	q.cond.L.Unlock()
	return entries
}

type UnblockingQueue struct {
	sync.Mutex
	entries []*Wrapper
}

func NewUnblockingQueue() (queue *UnblockingQueue) {
	queue = &UnblockingQueue{}
	queue.entries = make([]*Wrapper, 0, 1000)
	return
}

func (q *UnblockingQueue) Push(entry *Wrapper) {
	q.Lock()
	defer q.Unlock()
	q.entries = append(q.entries, entry)
}

func (q *UnblockingQueue) PopAll() []*Wrapper {
	q.Lock()
	defer q.Unlock()
	if len(q.entries) == 0 {
		return nil
	}
	entries := q.entries
	q.entries = make([]*Wrapper, 0, 1000)
	return entries
}
