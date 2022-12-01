package zdb

import (
	"os"
	"sync/atomic"

	zlog "github.com/zhangyu0310/zlogger"
)

type ZDB struct {
	memTable   *MemTable
	immTable   *MemTable
	walWriter  *LogWriter
	seqCounter uint64
}

func OpenDB() (*ZDB, error) {
	// TODO: config config config...
	file, err := os.OpenFile("./ZDB_WAL", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		zlog.Error("Open WAL failed. err:", err)
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		zlog.Error("Get WAL stat failed. err:", err)
		return nil, err
	}
	// TODO: Check WAL file to get data which not in SST
	// ...
	// Create ZDB
	db := &ZDB{
		memTable:   NewMemTable(&StringComparator{}),
		immTable:   nil,
		walWriter:  NewLogWriter(file, info.Size()),
		seqCounter: uint64(0), // FIXME: not always begin from 0
	}
	return db, nil
}

func (z *ZDB) Close() error {
	// TODO:
	return nil
}

func (z *ZDB) Get(key []byte) ([]byte, error) {
	// TODO:
	return nil, nil
}

func (z *ZDB) Put(key, value []byte) error {
	batch := &ZBatch{}
	batch.Put(key, value)
	return z.Write(batch)
}

func (z *ZDB) Delete(key []byte) error {
	batch := &ZBatch{}
	batch.Delete(key)
	return z.Write(batch)
}

func (z *ZDB) getNewSequence() uint64 {
	for {
		seq := z.seqCounter
		if atomic.CompareAndSwapUint64(&z.seqCounter, seq, seq+1) {
			if seq > SequenceMagicNum {
				zlog.Panic("Sequence is over 63 bit!")
			}
			return seq
		}
	}
}

func (z *ZDB) Write(batch *ZBatch) error {
	seq := z.getNewSequence()
	walData := make([]byte, 0, 2048)
	for _, item := range batch.item {
		data := z.memTable.Add(seq, item.Op, item.Value, item.Key)
		walData = append(walData, data...)
	}
	err := z.walWriter.AppendRecord(walData)
	if err != nil {
		zlog.Error("Append wal log failed, err:", err)
		return err
	}
	return nil
}
