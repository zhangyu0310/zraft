package zdb

const (
	OperatePut = ValueTypeValue
	OperateDel = ValueTypeDeletion
)

type BatchItem struct {
	Op    uint8
	Key   []byte
	Value []byte
}

type Batch struct {
	item []*BatchItem
}

func (batch *Batch) Put(key, value []byte) {
	localKey := make([]byte, len(key))
	copy(localKey, key)
	localValue := make([]byte, len(value))
	copy(localValue, value)
	batch.item = append(batch.item, &BatchItem{
		Op:    OperatePut,
		Key:   localKey,
		Value: localValue,
	})
}

func (batch *Batch) Delete(key []byte) {
	localKey := make([]byte, len(key))
	copy(localKey, key)
	batch.item = append(batch.item, &BatchItem{
		Op:    OperateDel,
		Key:   localKey,
		Value: nil,
	})
}
