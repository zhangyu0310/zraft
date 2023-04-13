package zdb

var (
	defaultOptions = &Options{
		DataDirPath:         ".",
		MaxHeightOfMemTable: 11,
		Comparator:          &StringComparator{},
	}

	defaultReadOptions = &ReadOptions{}

	defaultWriteOptions = &WriteOptions{
		Sync: false,
	}
)

type Options struct {
	DataDirPath         string
	MaxHeightOfMemTable int
	Comparator          Comparator
}

type ReadOptions struct {
}

type WriteOptions struct {
	Sync bool
}
