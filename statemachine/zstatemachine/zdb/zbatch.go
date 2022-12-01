package zdb

const (
	OperatePut = 0
	OperateDel = 1
)

type ZBatchItem struct {
	Op    uint8
	Key   []byte
	Value []byte
}

type ZBatch struct {
	item []*ZBatchItem
}

func (batch *ZBatch) Put(key, value []byte) {
	localKey := make([]byte, 32)
	copy(localKey, key)
	localValue := make([]byte, 32)
	copy(localValue, value)
	batch.item = append(batch.item, &ZBatchItem{
		Op:    OperatePut,
		Key:   localKey,
		Value: localValue,
	})
}

func (batch *ZBatch) Delete(key []byte) {
	localKey := make([]byte, 32)
	copy(localKey, key)
	batch.item = append(batch.item, &ZBatchItem{
		Op:    OperateDel,
		Key:   localKey,
		Value: nil,
	})
}
