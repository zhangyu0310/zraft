package zdb

import zlog "github.com/zhangyu0310/zlogger"

const (
	// SequenceMagicNum ---> 1111 1111
	SequenceMagicNum = 255
	// MaxSequenceNum only last 8 bit is 0
	MaxSequenceNum = uint64(0xFFFFFFFFFFFFFF00)
)

type InternalKey struct {
	key []byte
	// sequence [sequence] [deletion] = total 8 bytes
	//            7 byte     1 bytes
	sequence uint64
}

func NewInternalKey(userKey []byte, sequence uint64, valType uint8) *InternalKey {
	internalKey := &InternalKey{
		key:      userKey,
		sequence: sequence<<8 | uint64(valType),
	}
	return internalKey
}

func (key *InternalKey) GetUserKey() []byte {
	return key.key
}

func (key *InternalKey) GetSequenceNum() uint64 {
	return key.sequence >> 8
}

func (key *InternalKey) IsDeleted() bool {
	return (key.sequence & SequenceMagicNum) == ValueTypeDeletion
}

func (key *InternalKey) GetKeyType() uint8 {
	return uint8(key.sequence & SequenceMagicNum)
}

func InternalKeyEncode(key *InternalKey) []byte {
	data := make([]byte, 0, len(key.GetUserKey())+8)
	data = append(data, key.GetUserKey()...)
	fixedSeq := EncodeFixedUint64(key.sequence)
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
		key:      data[:dataLen-8],
		sequence: DecodeFixedUint64(seq),
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
		// Sequence bigger is the newer one, so return small value.
		if seqA > seqB {
			res = -1
		} else if seqA < seqB {
			res = 1
		}
	}
	return res
}

func (comp *InternalKeyComparator) CompareInternalKey(a *InternalKey, b *InternalKey) int {
	res := comp.UserComparator.Compare(a.GetUserKey(), b.GetUserKey())
	if res == 0 {
		if a.GetSequenceNum() > b.GetSequenceNum() {
			res = -1
		} else if a.GetSequenceNum() < b.GetSequenceNum() {
			res = 1
		}
	}
	return res
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
