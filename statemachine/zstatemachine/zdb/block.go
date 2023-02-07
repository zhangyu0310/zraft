package zdb

import (
	"errors"

	zlog "github.com/zhangyu0310/zlogger"
)

var (
	ErrBlockDataInvalid = errors.New("block data invalid")
	ErrRestartNumTooBig = errors.New("restart number too big")
	ErrIterTouchTheEnd  = errors.New("iter touch the end")
)

type Block struct {
	data          []byte
	size          int
	restartNum    int
	restartOffset uint32
}

type BlockIter struct {
	block         *Block
	dataOffset    uint32
	sharedKey     []byte
	restartOffset uint32
}

type RowData struct {
	sharedKeyLen    uint32
	nonSharedKeyLen uint32
	valueLen        uint32
	nonSharedKey    []byte
	value           []byte
}

func NewBlock(data []byte) (*Block, error) {
	size := len(data)
	if size < 4 {
		return nil, ErrBlockDataInvalid
	}
	b := &Block{
		data:          data,
		size:          size,
		restartNum:    0,
		restartOffset: 0,
	}
	fixedRestartNum := GetFixedUint32(b.data, b.size-4)
	b.restartNum = int(DecodeFixedUint32(fixedRestartNum))

	maxRestartNum := (b.size - 4) / 4
	if b.restartNum > maxRestartNum {
		return nil, ErrRestartNumTooBig
	}
	b.restartOffset = uint32(b.size) - 4*(uint32(b.restartNum)+1)
	return b, nil
}

func NewBlockIter(block *Block) *BlockIter {
	iter := &BlockIter{
		block:         block,
		dataOffset:    0,
		sharedKey:     nil,
		restartOffset: 0,
	}
	return iter
}

func parseBlockData(data []byte, offset uint32) (*RowData, uint32) {
	skLen, offset := GetVarUint64(data, offset)
	nskLen, offset := GetVarUint64(data, offset)
	vLen, offset := GetVarUint64(data, offset)
	sharedKeyLen := DecodeVarUint64(skLen)
	nonSharedKeyLen := DecodeVarUint64(nskLen)
	valueLen := DecodeVarUint64(vLen)

	nextOffset := offset + uint32(nonSharedKeyLen)
	nonSharedKey := data[offset:nextOffset]
	offset = nextOffset
	nextOffset = offset + uint32(valueLen)
	value := data[offset:nextOffset]
	return &RowData{
		sharedKeyLen:    uint32(sharedKeyLen),
		nonSharedKeyLen: uint32(nonSharedKeyLen),
		valueLen:        uint32(valueLen),
		nonSharedKey:    nonSharedKey,
		value:           value,
	}, nextOffset
}

func (iter *BlockIter) getRestartPoint() uint32 {
	fixedRestart := GetFixedUint32(iter.block.data, int(iter.restartOffset))
	restart := DecodeFixedUint32(fixedRestart)
	return restart
}

func (iter *BlockIter) Next() error {
	if iter.dataOffset >= iter.block.restartOffset {
		zlog.Debug("Iter next touch the end of block.")
		return ErrIterTouchTheEnd
	}
	// Get restart mode
	restartMode := false
	restart := iter.getRestartPoint()
	if restart <= iter.dataOffset {
		if iter.restartOffset+4 < uint32(iter.block.size-4) {
			// Not at the end of block
			iter.restartOffset += 4
			restart = iter.getRestartPoint()
		}
	}
	if restart == iter.dataOffset {
		restartMode = true
	}

	// Get row data
	rowData, offset := parseBlockData(iter.block.data, iter.dataOffset)
	iter.dataOffset = offset
	// Get a restart point, shared key length must be 0
	if restartMode {
		if rowData.sharedKeyLen != 0 {
			zlog.Error("Restart point but shared key length is not 0.")
			return ErrBlockDataInvalid
		}
		iter.sharedKey = rowData.nonSharedKey
	}
	return nil
}

func (iter *BlockIter) Get() ([]byte, []byte, error) {
	if iter.dataOffset >= iter.block.restartOffset {
		zlog.Debug("Iter get touch the end of block.")
		return nil, nil, ErrIterTouchTheEnd
	}
	rowData, _ := parseBlockData(iter.block.data, iter.dataOffset)
	key := make([]byte, 0, rowData.sharedKeyLen+rowData.nonSharedKeyLen)
	if rowData.sharedKeyLen == 0 {
		key = rowData.nonSharedKey
	} else {
		sharedKey := iter.sharedKey[0:rowData.sharedKeyLen]
		key = append(key, sharedKey...)
		key = append(key, rowData.nonSharedKey...)
	}
	return key, rowData.value, nil
}

func (row *RowData) Encode() []byte {
	data := make([]byte, 0, 1024)
	data = append(data, EncodeVarUint64(uint64(row.sharedKeyLen))...)
	data = append(data, EncodeVarUint64(uint64(row.nonSharedKeyLen))...)
	data = append(data, EncodeVarUint64(uint64(row.valueLen))...)
	data = append(data, row.nonSharedKey...)
	data = append(data, row.value...)
	return data
}
