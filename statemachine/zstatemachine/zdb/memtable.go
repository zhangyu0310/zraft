package zdb

import "errors"

var (
	ErrDataDeleted = errors.New("data deleted")
)

const (
	ValueTypeValue    = 0x0
	ValueTypeDeletion = 0x1
)

type MemTable struct {
	list       *SkipList
	comparator *MemTableKeyComparator
}

func NewMemTable(custom Comparator) *MemTable {
	memTable := &MemTable{
		list:       nil,
		comparator: NewMemTableKeyComparator(custom),
	}
	memTable.list = NewSkipList(func(a, b []byte) int {
		return memTable.comparator.Compare(a, b)
	})
	return memTable
}

func (table *MemTable) Get(key *LookupKey) (bool, []byte, error) {
	iter := NewSkipListIterator(table.list)
	iter.Seek(key.GetMemTableKey())
	if iter.Valid() {
		entry := iter.Key()
		varKeyLen, index := GetVarUint64(entry, 0)
		keyLen := DecodeVarUint64(varKeyLen)
		internalKey := InternalKeyDecode(entry[index : index+uint32(keyLen)])
		if table.comparator.keyComparator.UserComparator.Compare(
			internalKey.Key, key.GetUserKey()) == 0 {
			if internalKey.IsDeleted() {
				return true, nil, ErrDataDeleted
			}
			_, index = GetVarUint64(entry[index+uint32(keyLen):], index+uint32(keyLen))
			return true, entry[index:], nil
		}
	}
	return false, nil, nil
}

func (table *MemTable) Add(seq uint64, valueType uint8, key, value []byte) {
	keyLen := EncodeVarUint64(uint64(len(key) + 8))
	internalKey := NewInternalKey(key, seq, valueType)
	keyData := InternalKeyEncode(internalKey)
	valLen := EncodeVarUint64(uint64(len(value)))
	data := make([]byte, 0, len(keyLen)+len(key)+len(valLen)+len(value)+8)
	data = append(data, keyLen...)
	data = append(data, keyData...)
	data = append(data, valLen...)
	data = append(data, value...)
	table.list.Insert(data)
}

func (table *MemTable) AddData(data []byte) {
	table.list.Insert(data)
}

func (table *MemTable) GetCurrentHeight() int {
	return table.list.GetMaxHeight()
}

func (table *MemTable) Empty() bool {
	return table.list.head.Next(0) == nil
}

type MemTableKeyComparator struct {
	keyComparator *InternalKeyComparator
}

func NewMemTableKeyComparator(custom Comparator) *MemTableKeyComparator {
	return &MemTableKeyComparator{
		keyComparator: NewInternalKeyComparator(custom)}
}

func getLengthPrefixedData(data []byte) []byte {
	varLen, index := GetVarUint64(data, 0)
	dataLen := DecodeVarUint64(varLen)
	return data[index : index+uint32(dataLen)]
}

// Compare The params here are LookupKey.
func (comp *MemTableKeyComparator) Compare(a, b []byte) int {
	keyA := getLengthPrefixedData(a)
	keyB := getLengthPrefixedData(b)
	return comp.keyComparator.Compare(keyA, keyB)
}
