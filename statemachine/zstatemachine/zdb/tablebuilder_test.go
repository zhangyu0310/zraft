package zdb

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func makeBlockForTest(t *testing.T, begin, end, restartCount int) ([]byte, []byte) {
	builder := NewBlockBuilder()
	for i := begin; i < end; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := uuid.NewString()
		builder.Append([]byte(key), []byte(value))
	}
	// Build block
	data, last, err := builder.Build(restartCount)
	assert.Nil(t, err)
	return data, last
}

func TestTableBuilder_Build(t *testing.T) {
	tableBuilder, err := NewTableBuilder("./TestTableBuilderBuild")
	assert.Nil(t, err)

	totalTimes := 1000000
	for i := 0; i < 100; i++ {
		begin := totalTimes / 100 * i
		end := totalTimes / 100 * (i + 1)
		block, last := makeBlockForTest(t, begin, end, MaxRestartCount)
		err := tableBuilder.Append(block, last)
		assert.Nil(t, err)
	}
	err = tableBuilder.Build()
	assert.Nil(t, err)
}
