package zdb

var (
	defaultOptions = &Options{
		DataDirPath:         ".",
		MaxHeightOfMemTable: 11,
	}

	defaultReadOptions = &ReadOptions{}

	defaultWriteOptions = &WriteOptions{
		Sync: false,
	}
)

type Options struct {
	DataDirPath         string
	MaxHeightOfMemTable int
}

type ReadOptions struct {
}

type WriteOptions struct {
	Sync bool
}
