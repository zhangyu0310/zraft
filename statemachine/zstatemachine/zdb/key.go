package zdb

import zlog "github.com/zhangyu0310/zlogger"

// SequenceMagicNum ---> 0 [1 repeat 63 times]
const SequenceMagicNum = 255

type InternalKey struct {
	Key []byte
	// Sequence [sequence] [deletion] = total 8 bytes
	//            7 byte     1 bytes
	Sequence uint64
}

func NewInternalKey(userKey []byte, sequence uint64, valType uint8) *InternalKey {
	internalKey := &InternalKey{
		Key:      userKey,
		Sequence: sequence<<8 | uint64(valType),
	}
	return internalKey
}

func (key *InternalKey) GetSequenceNum() uint64 {
	return key.Sequence >> 8
}

func (key *InternalKey) IsDeleted() bool {
	return (key.Sequence & SequenceMagicNum) == ValueTypeDeletion
}

func InternalKeyEncode(key *InternalKey) []byte {
	data := make([]byte, 0, len(key.Key)+8)
	data = append(data, key.Key...)
	fixedSeq := EncodeFixedUint64(key.Sequence)
	data = append(data, fixedSeq[:]...)
	return data
}

func InternalKeyDecode(data []byte) *InternalKey {
	dataLen := len(data)
	if dataLen <= 8 {
		zlog.Panic("Internal key data length less than 8")
	}
	var seq FixedUint64
	copy(seq[:], data[dataLen-8:])
	key := &InternalKey{
		Key:      data[:dataLen-8],
		Sequence: DecodeFixedUint64(seq),
	}
	return key
}

type InternalKeyComparator struct {
	UserComparator Comparator
}

func NewInternalKeyComparator(custom Comparator) *InternalKeyComparator {
	return &InternalKeyComparator{UserComparator: custom}
}

func (comp *InternalKeyComparator) Name() string {
	return "ZDB.InternalKeyComparator"
}

func (comp *InternalKeyComparator) Compare(a, b []byte) int {
	res := comp.UserComparator.Compare(a[:len(a)-8], b[:len(b)-8])
	if res == 0 {
		var fixedA FixedUint64
		copy(fixedA[:], a[len(a)-8:])
		var fixedB FixedUint64
		copy(fixedB[:], b[len(a)-8:])
		seqA := DecodeFixedUint64(fixedA)
		seqB := DecodeFixedUint64(fixedB)
		if seqA > seqB {
			res = -1
		} else if seqA < seqB {
			res = 1
		}
	}
	return res
}

func (comp *InternalKeyComparator) CompareInternalKey(a *InternalKey, b *InternalKey) int {
	return comp.Compare(InternalKeyEncode(a), InternalKeyEncode(b))
}

type LookupKey struct {
	keyStart uint32
	data     []byte
}

func MakeLookupKey(userKey []byte, sequence uint64) *LookupKey {
	data := make([]byte, 0, len(userKey)+5+8)
	length := EncodeVarUint64(uint64(len(userKey) + 8))
	keyStart := len(length)
	data = append(data, length...)
	data = append(data, userKey...)
	tag := EncodeFixedUint64(sequence<<8 | ValueTypeValue)
	data = append(data, tag[:]...)
	return &LookupKey{
		keyStart: uint32(keyStart),
		data:     data,
	}
}

func (key *LookupKey) GetMemTableKey() []byte {
	return key.data
}

func (key *LookupKey) GetInternalKey() []byte {
	return key.data[key.keyStart:]
}

func (key *LookupKey) GetUserKey() []byte {
	return key.data[key.keyStart : len(key.data)-8]
}
