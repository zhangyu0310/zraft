package zdb

import (
	"errors"
	"os"

	zlog "github.com/zhangyu0310/zlogger"
)

const (
	MaxTableLevel = 6
)

var (
	ErrKeyTooSmall = errors.New("key is too small, not in table")
	ErrKeyTooBig   = errors.New("key is too big, not in table")
)

type TableMeta struct {
	TableIndex int
	Table      *os.File
	Meta       []*TableMetaInfo
	comparator *InternalKeyComparator
	MinKey     []byte
	MaxKey     []byte
}

type TableMetaInfo struct {
	BlockMaxKey []byte
	Handler     *BlockHandler
}

func NewTableMeta(index int, table *os.File, userComparator Comparator) *TableMeta {
	meta := &TableMeta{
		TableIndex: index,
		Table:      table,
		Meta:       make([]*TableMetaInfo, 0, 32),
		comparator: NewInternalKeyComparator(userComparator),
	}
	return meta
}

func NewTableMetaInfo(key []byte, offset, size uint64) *TableMetaInfo {
	info := &TableMetaInfo{
		BlockMaxKey: key,
		Handler: &BlockHandler{
			Offset: offset,
			Size:   size,
		},
	}
	return info
}

func (meta *TableMeta) Close() error {
	return meta.Table.Close()
}

func (meta *TableMeta) AddMetaInfo(key []byte, handler *BlockHandler) {
	meta.Meta = append(meta.Meta, NewTableMetaInfo(key, handler.Offset, handler.Size))
}

func (meta *TableMeta) Size() int {
	return len(meta.Meta)
}

func (meta *TableMeta) Find(key []byte) (*BlockHandler, error) {
	if meta.comparator.Compare(meta.MinKey, key) < 0 {
		return &BlockHandler{}, ErrKeyTooSmall
	}
	if meta.comparator.Compare(meta.MaxKey, key) > 0 {
		return &BlockHandler{}, ErrKeyTooBig
	}
	size := meta.Size()
	left := 0
	right := size
	mid := 0
	for left < right {
		mid = (left + right) / 2
		cmpRes := meta.comparator.Compare(meta.Meta[mid].BlockMaxKey, key)
		if cmpRes < 0 {
			right = mid
		} else if cmpRes > 0 {
			left = mid
		} else {
			break
		}
	}
	return meta.Meta[mid].Handler, nil
}

func (meta *TableMeta) GetBlock(handler *BlockHandler) (*Block, error) {
	tableName := meta.Table.Name()
	_, err := meta.Table.Seek(int64(handler.Offset), 0)
	if err != nil {
		zlog.ErrorF("Table [%s] seek failed, err: %s", tableName, err)
		return nil, err
	}
	blockData := make([]byte, handler.Size)
	_, err = meta.Table.Read(blockData)
	if err != nil {
		zlog.ErrorF("Table [%s] read failed, err: %s", tableName, err)
		return nil, err
	}
	block, err := NewBlock(blockData, meta.comparator.UserComparator)
	if err != nil {
		zlog.ErrorF("New block from table [%s] offset %d failed, err: %s",
			tableName, handler.Offset, err)
		return nil, err
	}
	return block, nil
}
