package zdb

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
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

	ManiFestFile = "MANIFEST"

	LevelTablePrefix = "ZDB_L"
)

type DB struct {
	memTable   *MemTable
	immTable   *MemTable
	immChanel  chan bool
	immPending atomic.Value
	immLock    *sync.Mutex
	immCond    *sync.Cond
	walWriter  *LogWriter
	seqCounter uint64
	options    *Options
	writeGroup []*Writer
	writeLock  *sync.Mutex
	tableIndex uint32
	tableMeta  [][]*TableMeta
	bgError    error
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

	// Get table file index from manifest file.
	tableIndex, err := GetTableIndexFromManifest(options.DataDirPath)
	if err != nil {
		zlog.Error("Get table index from manifest failed, err:", err)
		return nil, err
	}

	// Get all table meta info.
	tableMeta, err := GetTableMetaInfo(options)
	if err != nil {
		zlog.Error("Get table meta info failed, err:", err)
		return nil, err
	}

	// Create ZDB
	db := &DB{
		memTable:   NewMemTable(options.Comparator),
		immTable:   NewMemTable(options.Comparator),
		immChanel:  make(chan bool),
		immPending: atomic.Value{},
		immLock:    &sync.Mutex{},
		immCond:    nil,
		walWriter:  NewLogWriter(file, info.Size()),
		seqCounter: uint64(0), // Sequence will be updated when read WAL over.
		options:    options,
		writeGroup: make([]*Writer, 0, 16),
		writeLock:  &sync.Mutex{},
		tableIndex: tableIndex,
		tableMeta:  tableMeta,
		bgError:    nil,
	}
	db.immPending.Store(false)
	db.immCond = sync.NewCond(db.immLock)

	// Recover WAL data
	err = db.recoverDataFromWAL()
	if err != nil {
		zlog.Error("Recover data from WAL failed, err:", err)
		return nil, err
	}

	go db.minorCompaction()

	return db, nil
}

func GetTableIndexFromManifest(dirPath string) (uint32, error) {
	manifestPath := fmt.Sprintf("%s/%s", dirPath, ManiFestFile)
	manifest, err := os.OpenFile(manifestPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		zlog.Error("Open manifest file failed, err:", err)
		return 0, err
	}
	manifestSize, err := manifest.Stat()
	if err != nil {
		zlog.Error("Get stat of manifest file failed, err:", err)
		return 0, err
	}
	if manifestSize.Size() == 0 {
		zlog.Info("Manifest size is 0, need set something into it.")
		zero := EncodeFixedUint32(0)
		_, err = manifest.Write(zero[:])
		if err != nil {
			zlog.Error("Write zero into empty manifest file failed, err:", err)
			return 0, err
		}
		_, err = manifest.Seek(0, 0)
		if err != nil {
			zlog.Error("Seek to zero for manifest file failed, err:", err)
			return 0, err
		}
	}

	index := make([]byte, 4)
	_, err = manifest.Read(index)
	if err != nil {
		zlog.Error("Read manifest failed, err:", err)
		return 0, err
	}
	return DecodeFixedUint32(GetFixedUint32(index, 0)), nil
}

func GetTableMetaInfo(options *Options) ([][]*TableMeta, error) {
	// Collect all table files.
	dataFiles, err := ioutil.ReadDir(options.DataDirPath)
	if err != nil {
		zlog.Error("Read data dir files info failed, err:", err)
		return nil, err
	}
	tableMeta := make([][]*TableMeta, MaxTableLevel)
	for i := 0; i < MaxTableLevel; i++ {
		tableMeta[i] = make([]*TableMeta, 0, 16)
	}
	for _, dataFile := range dataFiles {
		fileName := dataFile.Name()
		if !dataFile.IsDir() &&
			strings.HasPrefix(fileName, LevelTablePrefix) {
			levelStr := fileName[len(LevelTablePrefix) : len(LevelTablePrefix)+1]
			level, err := strconv.Atoi(levelStr)
			if err != nil {
				zlog.ErrorF("Table name [%s] is invalid.", fileName)
				return nil, err
			}
			// Get table level & file index.
			indexStr := fileName[len(LevelTablePrefix)+len(".index-")+1:]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				zlog.Error("Table name [%s] is invalid.", fileName)
				return nil, err
			}
			metaVec := tableMeta[level]
			tableFile, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
			if err != nil {
				zlog.ErrorF("Open table file %s failed, err: %s", fileName, err)
				return nil, err
			}
			// Read footer to find out the position of index block.
			_, err = tableFile.Seek(ConstFooterSize, 2)
			if err != nil {
				zlog.ErrorF("Table file %s seek to footer failed, err: %s", fileName, err)
				return nil, err
			}
			footerData := make([]byte, ConstFooterSize)
			_, err = tableFile.Read(footerData)
			if err != nil {
				zlog.ErrorF("Table file %s read footer failed, err: %s", fileName, err)
				return nil, err
			}
			footer := DecodeFooter(footerData)
			_, err = tableFile.Seek(int64(footer.IndexBlock.Offset), 0)
			if err != nil {
				zlog.ErrorF("Table file %s seek to index block failed, err: %s", fileName, err)
				return nil, err
			}
			// Get index block & storage into meta info.
			indexBlockData := make([]byte, footer.IndexBlock.Size)
			_, err = tableFile.Read(indexBlockData)
			if err != nil {
				zlog.ErrorF("Table file %s read index block from offset %d failed, err: %s",
					fileName, footer.IndexBlock.Offset, err)
				return nil, err
			}
			indexBlock, err := NewBlock(indexBlockData, options.Comparator)
			if err != nil {
				zlog.ErrorF("New table %s index block failed, err: %s", fileName, err)
				return nil, err
			}
			indexBlockIter := NewBlockIter(indexBlock)
			indexBlockIter.SeekToFirst()
			var minKey, maxKey []byte
			meta := NewTableMeta(index, tableFile, options.Comparator)
			for indexBlockIter.Valid() {
				key, value, err := indexBlockIter.Get()
				if err != nil {
					zlog.Error("Get index block key/value failed, err:", err)
					return nil, err
				}
				if minKey == nil {
					minKey = key
				}
				bh := DecodeBlockHandler(value)
				meta.AddMetaInfo(key, bh)
				err = indexBlockIter.Next()
				if err != nil {
					if err == ErrIterTouchTheEnd {
						maxKey = key
						break
					}
					zlog.Error("Index block get next failed, err:", err)
					return nil, err
				}
			}
			meta.MinKey = minKey
			meta.MaxKey = maxKey
			metaVec = append(metaVec, meta)
		}
	}
	for _, metaVec := range tableMeta {
		sort.Slice(metaVec, func(i, j int) bool {
			return metaVec[i].TableIndex > metaVec[j].TableIndex
		})
	}
	return tableMeta, nil
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
			targetTable.Add(internalKey.GetSequenceNum(),
				internalKey.GetKeyType(),
				internalKey.GetUserKey(),
				value)
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

func searchTableMap(lookup *LookupKey, tm []*TableMeta) (bool, []byte, []byte, error) {
	for _, meta := range tm {
		handler, err := meta.Find(lookup.GetInternalKey())
		if err != nil {
			zlog.Debug("Find table meta get %s, key is not in this table", err)
			continue
		}
		block, err := meta.GetBlock(handler)
		if err != nil {
			zlog.ErrorF("Get table index [%d] block from handler %v failed, err: %s",
				meta.TableIndex, handler, err)
			return false, nil, nil, err
		}
		exist, tKey, tValue, err := block.Get(lookup)
		if err != nil {
			if err == ErrDataDeleted {
				return true, nil, nil, ErrDataDeleted
			} else {
				zlog.Error("Find in block failed, err:", err)
				return false, nil, nil, err
			}
		}
		if exist {
			return true, tKey, tValue, nil
		}
	}
	return false, nil, nil, nil
}

func (z *DB) Get(key []byte) ([]byte, error) {
	lookup := MakeLookupKey(key, z.seqCounter)
	// Search mem table
	exist, _, value, err := z.memTable.Get(lookup)
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
	exist, _, value, err = z.immTable.Get(lookup)
	if err != nil {
		if errors.Is(err, ErrDataDeleted) {
			return nil, ErrKeyIsNotExist
		}
		return nil, err
	}
	if exist {
		return value, nil
	}
	// Search table files
	for level, tm := range z.tableMeta {
		zlog.Debug("Search level", level, "table files")
		exist, _, value, err = searchTableMap(lookup, tm)
		if err != nil {
			if errors.Is(err, ErrDataDeleted) {
				return nil, ErrKeyIsNotExist
			}
			return nil, err
		}
		if exist {
			return value, nil
		}
	}
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

func (z *DB) minorCompaction() {
	for {
		select {
		case <-z.immChanel:
			z.immLock.Lock()
			z.immPending.Store(true)
			z.makeImmToL0()
			z.immPending.Store(false)
			z.immLock.Unlock()
			z.immCond.Signal()
		}
	}
}

func (z *DB) makeImmToL0() {
	tablePath := fmt.Sprintf("%s/%s%d.index-%d", z.options.DataDirPath, LevelTablePrefix, 0, z.tableIndex)
	tb, err := NewTableBuilder(tablePath)
	if err != nil {
		zlog.ErrorF("New L0 table builder [%s] failed, err: %s", tablePath, err)
		z.bgError = err
		return
	}
	defer func(tb *TableBuilder) {
		err := tb.Close()
		if err != nil {
			zlog.Error("Close L0 table builder failed, err:", err)
		}
	}(tb)

	z.tableIndex++
	manifestPath := fmt.Sprintf("%s/%s", z.options.DataDirPath, ManiFestFile)
	manifest, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0666)
	if err != nil {
		zlog.Error("Modify manifest file failed, err:", err)
		z.bgError = err
		return
	}
	defer func(manifest *os.File) {
		err := manifest.Close()
		if err != nil {
			zlog.Error("Close manifest file failed, err:", err)
		}
	}(manifest)
	index := EncodeFixedUint32(z.tableIndex)
	num, err := manifest.Write(index[:])
	if err != nil || num != 4 {
		zlog.Error("Write manifest file failed, err:", err)
		z.bgError = err
		return
	}

	defer func() {
		if err != nil {
			z.bgError = err
			zlog.Error("Make imm to L0 failed, err:", err)
			_ = os.Remove(tablePath)
		}
	}()

	immIter := NewMemTableIterator(z.immTable)
	bb := NewBlockBuilder()
	immIter.SeekToFirst()
	for immIter.Valid() {
		bb.Append(immIter.Get())
		if bb.Size() > NormalBlockSize {
			var data, lastKey []byte
			data, lastKey, err = bb.Build(MaxRestartCount)
			if err != nil {
				zlog.Error("Block builder build failed, err:", err)
				return
			}
			err = tb.Append(data, lastKey)
			if err != nil {
				zlog.Error("Table builder append failed, err:", err)
				return
			}
			bb = NewBlockBuilder()
		}
		immIter.Next()
	}

	err = tb.Build()
	if err != nil {
		zlog.Error("Table builder build failed, err:", err)
		return
	}
	z.immTable = nil
	immWALPath := fmt.Sprintf("%s/%s", z.options.DataDirPath, NameOfImmWAL)
	_ = os.Remove(immWALPath)
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
		z.immLock.Lock()
		for z.immPending.Load().(bool) {
			z.immCond.Wait()
		}
		if z.bgError != nil {
			return z.bgError
		}

		// Update memTable WAL file name, make new memTable
		walWriter, err := UpdateWALFile(z.walWriter, z.options)
		if err != nil {
			zlog.Error("Update WAL file failed, err:", err)
			return err
		}
		z.walWriter = walWriter
		z.immTable = z.memTable
		z.memTable = NewMemTable(z.options.Comparator)
		z.immChanel <- true
		z.immLock.Unlock()
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
