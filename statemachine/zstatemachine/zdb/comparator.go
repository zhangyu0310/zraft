package zdb

type Comparator interface {
	Name() string
	Compare([]byte, []byte) int
}

type StringComparator struct{}

func (comp *StringComparator) Name() string {
	return "ZDB.StringComparator"
}

func (comp *StringComparator) Compare(a, b []byte) int {
	strA := string(a)
	strB := string(b)
	if strA == strB {
		return 0
	}
	if strA > strB {
		return +1
	} else {
		return -1
	}
}
