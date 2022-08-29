package zstatemachine

const (
	OperatePut = 0
	OperateDel = 1
)

type ZRaftBatchItem struct {
	Op    int8
	Key   []byte
	Value []byte
}

type ZRaftBatch struct {
	item []*ZRaftBatchItem
}

func (batch *ZRaftBatch) Put(key, value []byte) {
	localKey := make([]byte, 32)
	copy(localKey, key)
	localValue := make([]byte, 32)
	copy(localValue, value)
	batch.item = append(batch.item, &ZRaftBatchItem{
		Op:    OperatePut,
		Key:   localKey,
		Value: localValue,
	})
}

func (batch *ZRaftBatch) Delete(key []byte) {
	localKey := make([]byte, 32)
	copy(localKey, key)
	batch.item = append(batch.item, &ZRaftBatchItem{
		Op:    OperateDel,
		Key:   localKey,
		Value: nil,
	})
}
