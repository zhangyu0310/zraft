package zdb

import (
	"errors"

	zlog "github.com/zhangyu0310/zlogger"
)

var (
	ErrBlockIsEmpty = errors.New("block is empty")
)

const (
	NormalBlockSize = 4096
	MaxRestartCount = 16
)

type BlockBuilder struct {
	data []*RowData
}

func NewBlockBuilder() *BlockBuilder {
	builder := &BlockBuilder{
		data: make([]*RowData, 0, 64),
	}
	return builder
}

func (builder *BlockBuilder) Append(key, value []byte) {
	builder.data = append(builder.data, &RowData{
		sharedKeyLen:    0,
		nonSharedKeyLen: uint32(len(key)),
		valueLen:        uint32(len(value)),
		nonSharedKey:    key,
		value:           value,
	})
}

func getSameLengthWithSharedKey(sharedKey, data []byte) uint32 {
	length := uint32(0)
	for {
		// One of the byte vector reach the end.
		if uint32(len(sharedKey)) == length || uint32(len(data)) == length {
			break
		}
		// Data byte vector have different byte with shared key byte vector.
		if sharedKey[length] != data[length] {
			break
		}
		length++
	}
	return length
}

func (builder *BlockBuilder) Build(maxRestartCount int) ([]byte, []byte, error) {
	if builder.data == nil || builder.Size() == 0 {
		zlog.Info("There is an empty block builder call function Build().")
		return nil, nil, ErrBlockIsEmpty
	}

	totalData := make([]byte, 0, 10240)
	// Get restart points
	restartPoints := make([]uint32, 0, 32)
	restartCount := maxRestartCount
	restartOffset := uint32(0)
	var sharedKey []byte
	var lastKey []byte
	// Encode block rows
	for _, row := range builder.data {
		lastKey = row.nonSharedKey
		sharedLength := uint32(0)
		if restartCount >= maxRestartCount {
			sharedKey = row.nonSharedKey
			lastRestartPoint := uint32(0)
			if len(restartPoints) > 0 {
				lastRestartPoint = restartPoints[len(restartPoints)-1]
			}
			restartPoints = append(restartPoints, lastRestartPoint+restartOffset)
			restartCount = 0
			restartOffset = 0
		} else {
			sharedLength = getSameLengthWithSharedKey(sharedKey, row.nonSharedKey)
		}
		restartCount++
		row.sharedKeyLen = sharedLength
		row.nonSharedKeyLen -= sharedLength
		row.nonSharedKey = row.nonSharedKey[sharedLength:]
		rowData := row.Encode()
		restartOffset += uint32(len(rowData))
		totalData = append(totalData, rowData...)
	}
	for _, rp := range restartPoints {
		erp := EncodeFixedUint32(rp)
		totalData = append(totalData, erp[:]...)
	}
	rpSize := EncodeFixedUint32(uint32(len(restartPoints)))
	totalData = append(totalData, rpSize[:]...)
	return totalData, lastKey, nil
}

func (builder *BlockBuilder) Size() uint32 {
	totalSize := uint32(0)
	for _, data := range builder.data {
		totalSize += 3 * 8
		totalSize += uint32(len(data.nonSharedKey))
		totalSize += uint32(len(data.value))
	}
	return totalSize
}
