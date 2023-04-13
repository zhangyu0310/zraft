package zdb

import "strings"

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
	return strings.Compare(strA, strB)
}
