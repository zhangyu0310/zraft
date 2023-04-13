package zdb

import (
	"hash/crc64"
	"os"

	zlog "github.com/zhangyu0310/zlogger"
)

const (
	ConstIndexBlockHandlerSize = 10 + 10
	ConstFooterMagicNumberSize = 8
	ConstLastPaddingForFooter  = 4
	ConstFooterSize            = ConstIndexBlockHandlerSize + ConstFooterMagicNumberSize + ConstLastPaddingForFooter
)

type BlockHandler struct {
	Offset uint64
	Size   uint64
}

type Footer struct {
	IndexBlock BlockHandler
	MagicNum   uint64
}

type TableBuilder struct {
	TableFile   *os.File
	BlockOffset uint64
	IndexBlock  *BlockBuilder
	Footer      *Footer
}

func DecodeFooter(data []byte) *Footer {
	index := uint32(0)
	var varOffset VarUint64
	var varSize VarUint64
	var fixedMagic FixedUint64
	varOffset, index = GetVarUint64(data, index)
	varSize, index = GetVarUint64(data, index)
	offset := DecodeVarUint64(varOffset)
	size := DecodeVarUint64(varSize)

	copy(fixedMagic[:], data[len(data)-8:])
	magicNum := DecodeFixedUint64(fixedMagic)
	return &Footer{
		IndexBlock: BlockHandler{
			Offset: offset,
			Size:   size,
		},
		MagicNum: magicNum,
	}
}

func (footer *Footer) Encode() []byte {
	data := make([]byte, 0, ConstFooterSize)
	data = append(data, EncodeVarUint64(footer.IndexBlock.Offset)...)
	data = append(data, EncodeVarUint64(footer.IndexBlock.Size)...)
	// Add some padding, index block meta info max is 20 bytes.
	paddingSize := ConstIndexBlockHandlerSize - len(data)
	if paddingSize > 0 {
		data = append(data, make([]byte, paddingSize)...)
	}
	fixedMagic := EncodeFixedUint64(footer.MagicNum)
	data = append(data, fixedMagic[:]...)
	data = append(data, make([]byte, ConstLastPaddingForFooter)...)
	return data
}

func DecodeBlockHandler(data []byte) *BlockHandler {
	index := uint32(0)
	var (
		varOffset VarUint64
		varSize   VarUint64
	)
	varOffset, index = GetVarUint64(data, index)
	varSize, index = GetVarUint64(data, index)
	return &BlockHandler{
		Offset: DecodeVarUint64(varOffset),
		Size:   DecodeVarUint64(varSize),
	}
}

func (bh *BlockHandler) Encode() []byte {
	data := make([]byte, 0, 20)
	data = append(data, EncodeVarUint64(bh.Offset)...)
	data = append(data, EncodeVarUint64(bh.Size)...)
	return data
}

func NewTableBuilder(tablePath string) (*TableBuilder, error) {
	file, err := os.OpenFile(tablePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		zlog.ErrorF("Open table file [%s] failed, err: %s", tablePath, err)
		return nil, err
	}
	return &TableBuilder{
		TableFile:   file,
		BlockOffset: 0,
		IndexBlock:  NewBlockBuilder(),
		Footer: &Footer{
			IndexBlock: BlockHandler{
				Offset: 0,
				Size:   0,
			},
			MagicNum: 0,
		},
	}, nil
}

func (tb *TableBuilder) Append(blockData []byte, blockMaxKey []byte) error {
	size, err := tb.TableFile.Write(blockData)
	if err != nil {
		zlog.ErrorF("Table file [%s] write failed, write size %d, err: %s",
			tb.TableFile.Name(), size, err)
		return err
	}

	bh := BlockHandler{
		Offset: tb.BlockOffset,
		Size:   uint64(len(blockData)),
	}
	tb.BlockOffset += uint64(len(blockData))
	tb.IndexBlock.Append(blockMaxKey, bh.Encode())
	return nil
}

func (tb *TableBuilder) Build() error {
	data, _, err := tb.IndexBlock.Build(1)
	if err != nil {
		zlog.Error("Table builder index block build failed, err:", err)
		return err
	}
	size, err := tb.TableFile.Write(data)
	if err != nil {
		zlog.ErrorF("Table file [%s] write index block failed, write size %d, err: %s",
			tb.TableFile.Name(), size, err)
		return err
	}
	tb.Footer.IndexBlock.Offset = tb.BlockOffset
	tb.Footer.IndexBlock.Size = uint64(len(data))
	tb.Footer.MagicNum = crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
	footerData := tb.Footer.Encode()
	size, err = tb.TableFile.Write(footerData)
	if err != nil {
		zlog.ErrorF("Table file [%s] write footer failed, write size %d, err: %s",
			tb.TableFile.Name(), size, err)
		return err
	}
	err = tb.TableFile.Sync()
	if err != nil {
		zlog.ErrorF("Table file [%s] sync failed, err: %s", tb.TableFile.Name(), err)
		return err
	}
	return nil
}

func (tb *TableBuilder) Close() error {
	return tb.TableFile.Close()
}
