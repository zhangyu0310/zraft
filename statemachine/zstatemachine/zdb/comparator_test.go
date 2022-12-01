package zdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringComparator_Name(t *testing.T) {
	comp := StringComparator{}
	name := comp.Name()
	assert.Equal(t, "ZDB.StringComparator", name)
}

func TestStringComparator_Compare(t *testing.T) {
	comp := StringComparator{}
	assert.Equal(t, 0, comp.Compare([]byte("test"), []byte("test")))
	assert.Equal(t, +1, comp.Compare([]byte("test2"), []byte("test1")))
	assert.Equal(t, -1, comp.Compare([]byte("test1"), []byte("test2")))
}

func TestStringComparator_Compare2(t *testing.T) {
	comp := StringComparator{}
	dataA := make([]byte, 0, 16)
	dataB := make([]byte, 0, 16)
	for i := 0; i < 16; i++ {
		dataA = append(dataA, byte(i))
	}
	for i := 15; i > 0; i-- {
		dataB = append(dataB, byte(i))
	}
	data := make([]byte, 16)
	assert.Equal(t, 0, comp.Compare(data, data))
	assert.Equal(t, -1, comp.Compare(dataA, dataB))
	assert.Equal(t, +1, comp.Compare(dataB, dataA))
}
