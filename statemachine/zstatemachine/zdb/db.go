package zdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	zlog "github.com/zhangyu0310/zlogger"
)

var (
	ErrKeyIsNotExist = errors.New("key is not exist")
)

const (
	NameOfMemWAL = "ZDB_WAL_MEM"
	NameOfImmWAL = "ZDB_WAL_IMM"

	L0TablePrefix = "ZDB_L0"
)

type DB struct {
	memTable     *MemTable
	immTable     *MemTable
	walWriter    *LogWriter
	seqCounter   uint64
	options      *Options
	writeGroup   []*Writer
	writeLock    *sync.Mutex
	tableL0Index uint32
	tableMeta    []map[string]*TableMeta
	bgError      error
}

type Writer struct {
	batch *Batch
	cond  *sync.Cond
	sync  bool
	done  bool
	err   error
}

func OpenDB(options *Options) (*DB, error) {
	if options == nil {
		options = defaultOptions
	}
	pathOfMemWAL := fmt.Sprintf("%s/%s", options.DataDirPath, NameOfMemWAL)
	// Get mem wal offset for wal writer.
	file, err := os.OpenFile(pathOfMemWAL, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		zlog.Error("Open WAL failed. err:", err)
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		zlog.Error("Get WAL stat failed. err:", err)
		return nil, err
	}
	// Create ZDB
	db := &DB{
		memTable:     NewMemTable(&StringComparator{}),
		immTable:     NewMemTable(&StringComparator{}),
		walWriter:    NewLogWriter(file, info.Size()),
		seqCounter:   uint64(0), // Sequence will be updated when read WAL over.
		options:      options,
		writeGroup:   make([]*Writer, 0, 16),
		writeLock:    &sync.Mutex{},
		tableL0Index: 0,
		tableMeta:    make([]map[string]*TableMeta, MaxTableLevel),
		bgError:      nil,
	}
	for i := 0; i < MaxTableLevel; i++ {
		db.tableMeta[i] = make(map[string]*TableMeta)
	}
	err = db.recoverDataFromWAL()
	if err != nil {
		zlog.Error("Recover data from WAL failed, err:", err)
		return nil, err
	}
	return db, nil
}

func recoverDataInternal(filePath string, targetTable *MemTable) (uint64, error) {
	wal, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		zlog.ErrorF("Open WAL file [%s] failed, err: %s", filePath, err)
		return 0, err
	}
	walReader, err := NewLogReader(wal, 0)
	if err != nil {
		zlog.Error("Create new log reader failed, err", err)
		return 0, err
	}
	defer func(walReader *LogReader) {
		err := walReader.Close()
		if err != nil {
			zlog.Error("Close WAL reader failed, err:", err)
		}
	}(walReader)
	lastSeq := uint64(0)
	for {
		record, err := walReader.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			zlog.Error("Log reader read record failed, err:", err)
			return 0, err
		}
		index := uint32(0)
		for index < uint32(len(record)) {
			var key []byte
			_, key, index = GetLengthAndValue(record, index)
			var value []byte
			_, value, index = GetLengthAndValue(record, index)
			internalKey := InternalKeyDecode(key)
			targetTable.Add(internalKey.GetSequenceNum(), internalKey.GetKeyType(), internalKey.Key, value)
			lastSeq = internalKey.GetSequenceNum()
		}
	}
	return lastSeq, nil
}

func (z *DB) recoverDataFromWAL() error {
	memWalPath := fmt.Sprintf("%s/%s", z.options.DataDirPath, NameOfMemWAL)
	zlog.Info("Mem WAL path is", memWalPath)
	lastSeq, err := recoverDataInternal(memWalPath, z.memTable)
	if err != nil {
		zlog.Error("Recover data for mem table failed, err:", err)
		return err
	}
	immWalPath := fmt.Sprintf("%s/%s", z.options.DataDirPath, NameOfImmWAL)
	zlog.Info("Imm WAL path is", immWalPath)
	if _, err := recoverDataInternal(immWalPath, z.immTable); err != nil {
		if os.IsNotExist(err) {
			zlog.Info("Imm WAL file is not exist, maybe it have been purged.")
		} else {
			zlog.Error("Recover data for imm table failed, err:", err)
			return err
		}
	}
	z.seqCounter = lastSeq
	return nil
}

func (z *DB) Close() error {
	err := z.walWriter.Close()
	if err != nil {
		zlog.Error("Close WAL file handler failed, err:", err)
		return err
	}
	return nil
}

func (z *DB) Get(key []byte) ([]byte, error) {
	lookup := MakeLookupKey(key, z.seqCounter)
	// Search mem table
	exist, value, err := z.memTable.Get(lookup)
	if err != nil {
		if errors.Is(err, ErrDataDeleted) {
			return nil, ErrKeyIsNotExist
		}
		return nil, err
	}
	if exist {
		return value, nil
	}
	// Search imm table
	exist, value, err = z.immTable.Get(lookup)
	if err != nil {
		if errors.Is(err, ErrDataDeleted) {
			return nil, ErrKeyIsNotExist
		}
		return nil, err
	}
	if exist {
		return value, nil
	}
	// Search L0 files

	return nil, ErrKeyIsNotExist
}

func (z *DB) Put(key, value []byte, options *WriteOptions) error {
	batch := &Batch{}
	batch.Put(key, value)
	return z.Write(batch, options)
}

func (z *DB) Delete(key []byte, options *WriteOptions) error {
	batch := &Batch{}
	batch.Delete(key)
	return z.Write(batch, options)
}

func (z *DB) getNewSequence() uint64 {
	for {
		seq := z.seqCounter
		if atomic.CompareAndSwapUint64(&z.seqCounter, seq, seq+1) {
			if seq > MaxSequenceNum {
				zlog.Panic("sequence is over 63 bit!")
			}
			return seq
		}
	}
}

func makeData(seq uint64, valueType uint8, key, value []byte) []byte {
	keyLen := EncodeVarUint64(uint64(len(key) + 8))
	internalKey := NewInternalKey(key, seq, valueType)
	keyData := InternalKeyEncode(internalKey)
	valLen := EncodeVarUint64(uint64(len(value)))
	data := make([]byte, 0, len(keyLen)+len(key)+len(valLen)+len(value)+8)
	data = append(data, keyLen...)
	data = append(data, keyData...)
	data = append(data, valLen...)
	data = append(data, value...)
	return data
}

func (z *DB) makeImmToL0(tableName string) error {
	tb, err := NewTableBuilder(tableName)
	if err != nil {
		zlog.ErrorF("New L0 table builder [%s] failed, err: %s", tableName, err)
		return err
	}

	immIter := NewMemTableIterator(z.immTable)
	bb := NewBlockBuilder()
	immIter.SeekToFirst()
	for immIter.Valid() {
		bb.Append(immIter.Get())
		if bb.Size() > NormalBlockSize {
			data, lastKey, err := bb.Build(MaxRestartCount)
			if err != nil {
				zlog.Error("Block builder build failed, err:", err)
				return err
			}
			err = tb.Append(data, lastKey)
			if err != nil {
				zlog.Error("Table builder append failed, err:", err)
				return err
			}
			bb = NewBlockBuilder()
		}
		immIter.Next()
	}

	err = tb.Build()
	if err != nil {
		zlog.Error("Table builder build failed, err:", err)
		return err
	}
	z.tableL0Index++
	z.immTable = nil
	immWALPath := fmt.Sprintf("%s/%s", z.options.DataDirPath, NameOfImmWAL)
	_ = os.Remove(immWALPath)
	return nil
}

func (z *DB) makeSomeSpace() error {
	if z.bgError != nil {
		zlog.Error("DB have bg error:", z.bgError)
		return z.bgError
	}
	walStat, err := z.walWriter.Size()
	if err != nil {
		zlog.Error("Get WAL file stat failed, err:", err)
		return err
	}
	if walStat > MaxWALFileSize {
		// Move memTable to immTable need a lot of time.
		// So unlock for other routine to put their writer into writeGroup.
		z.writeLock.Unlock()
		defer z.writeLock.Lock()
		if !z.immTable.Empty() {
			tableName := fmt.Sprintf("%s.index-%d", L0TablePrefix, z.tableL0Index)
			err := z.makeImmToL0(tableName)
			if err != nil {
				zlog.Error("Make imm to L0 failed, err:", err)
				_ = os.Remove(tableName)
				return err
			}
		}
		// Update memTable WAL file name, make new memTable
		walWriter, err := UpdateWALFile(z.walWriter, z.options)
		if err != nil {
			zlog.Error("Update WAL file failed, err:", err)
			return err
		}
		z.walWriter = walWriter
		z.immTable = z.memTable
		z.memTable = NewMemTable(&StringComparator{})
	}
	return nil
}

func (z *DB) Write(batch *Batch, options *WriteOptions) error {
	if options == nil {
		options = defaultWriteOptions
	}
	// Make Writer and insert it into write group.
	w := &Writer{
		batch: batch,
		cond:  sync.NewCond(z.writeLock),
		sync:  options.Sync,
		done:  false,
		err:   nil,
	}
	z.writeLock.Lock()
	defer z.writeLock.Unlock()
	// DB had been locked, safe to get/set writerGroup now.
	z.writeGroup = append(z.writeGroup, w)
	for !w.done && z.writeGroup[0] != w {
		w.cond.Wait()
	}
	if w.done {
		return w.err
	}
	err := z.makeSomeSpace()
	if err != nil {
		zlog.Error("Make space failed, err:", err)
	}
	// last writer must be self, because error handle must have a valid lastWriter
	lastWriter := w
	// Make some space success, handle insert data.
	if err == nil {
		allData := make([]byte, 0, 1024)
		dataVec := make([][]byte, 0, 16)
		for _, writer := range z.writeGroup {
			if writer.sync != options.Sync {
				break
			}
			seq := z.getNewSequence()
			for _, item := range writer.batch.item {
				data := makeData(seq, item.Op, item.Key, item.Value)
				allData = append(allData, data...)
				dataVec = append(dataVec, data)
			}
			lastWriter = writer
		}
		// Get batch group data success, so unlock writeGroup let other thread insert writer.
		z.writeLock.Unlock()
		syncFail := false
		err = z.walWriter.AppendRecord(allData)
		if err == nil {
			if options.Sync {
				err = z.walWriter.Sync()
				if err != nil {
					syncFail = true
				}
			}
		}
		if err == nil {
			for _, data := range dataVec {
				z.memTable.AddData(data)
			}
		}
		z.writeLock.Lock()
		if syncFail {
			z.bgError = err
		}
	}
	// Signal other routine and handle error.
	for {
		handleWriter := z.writeGroup[0]
		z.writeGroup = z.writeGroup[1:]
		if handleWriter != w {
			handleWriter.err = err
			handleWriter.done = true
			handleWriter.cond.Signal()
		}
		if handleWriter == lastWriter {
			break
		}
	}
	if len(z.writeGroup) != 0 {
		z.writeGroup[0].cond.Signal()
	}
	return nil
}
