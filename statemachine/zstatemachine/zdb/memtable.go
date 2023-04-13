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
	memTable.list = NewSkipList(memTable.comparator.Compare)
	return memTable
}

func (table *MemTable) Get(key *LookupKey) (bool, []byte, []byte, error) {
	iter := NewSkipListIterator(table.list)
	iter.Seek(key.GetMemTableKey())
	if iter.Valid() {
		entry := iter.Key()
		varKeyLen, index := GetVarUint64(entry, 0)
		keyLen := DecodeVarUint64(varKeyLen)
		keyData := entry[index : index+uint32(keyLen)]
		internalKey := InternalKeyDecode(keyData)
		if table.comparator.keyComparator.UserComparator.Compare(
			internalKey.GetUserKey(), key.GetUserKey()) == 0 {
			if internalKey.IsDeleted() {
				return true, keyData, nil, ErrDataDeleted
			}
			_, index = GetVarUint64(entry, index+uint32(keyLen))
			return true, keyData, entry[index:], nil
		}
	}
	return false, nil, nil, nil
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

type MemTableIterator struct {
	table  *MemTable
	slIter *SkipListIterator
}

func NewMemTableIterator(table *MemTable) *MemTableIterator {
	return &MemTableIterator{
		table:  table,
		slIter: NewSkipListIterator(table.list),
	}
}

func (iter *MemTableIterator) Valid() bool {
	return iter.slIter.Valid()
}

func (iter *MemTableIterator) Get() (key, value []byte) {
	if iter.Valid() {
		entry := iter.slIter.Key()
		index := uint32(0)
		_, key, index = GetLengthAndValue(entry, index)
		_, value, index = GetLengthAndValue(entry, index)
	}
	return
}

func (iter *MemTableIterator) Next() {
	iter.slIter.Next()
}

func (iter *MemTableIterator) Prev() {
	iter.slIter.Prev()
}

func (iter *MemTableIterator) Seek(key []byte) {
	lookup := MakeLookupKey(key, MaxSequenceNum)
	iter.slIter.Seek(lookup.GetMemTableKey())
}

func (iter *MemTableIterator) SeekToFirst() {
	iter.slIter.SeekToFirst()
}

func (iter *MemTableIterator) SeekToLast() {
	iter.slIter.SeekToLast()
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
