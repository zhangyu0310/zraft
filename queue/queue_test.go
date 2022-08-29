package queue

import (
	"fmt"
	"sync"
	"testing"

	"zraft/log"
)

func TestBlockingQueue(t *testing.T) {
	queue := NewBlockingQueue()

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			for j := 0; j < 10; j++ {
				queue.Push(&log.Entry{
					Term:  uint64(i),
					Op:    log.OpInvalid,
					Key:   []byte(fmt.Sprintf("%d:%d", i, j)),
					Value: []byte("test"),
				})
			}
			wg.Done()
		}()
	}

	wg.Wait()

	entries := queue.PopAll()
	for _, entry := range entries {
		fmt.Printf("Go %s\n", entry.Key)
	}
	if len(entries) != 1000 {
		t.Error("ERROR! Length of entries are not 1000.")
	}
}
