package zdb

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeFixedUint16(t *testing.T) {
	fixed := EncodeFixedUint16(16)
	assert.Equal(t, 2, len(fixed))
	assert.Equal(t, byte(16), fixed[0])
	assert.Equal(t, byte(0), fixed[1])
}

func TestDecodeFixedUint16(t *testing.T) {
	r := rand.Intn(32767)
	fixed := EncodeFixedUint16(uint16(r))
	res := DecodeFixedUint16(fixed)
	assert.Equal(t, uint16(r), res)
}

func TestEncodeFixedUint32(t *testing.T) {
	fixed := EncodeFixedUint32(32)
	assert.Equal(t, 4, len(fixed))
	assert.Equal(t, byte(32), fixed[0])
	assert.Equal(t, byte(0), fixed[1])
	assert.Equal(t, byte(0), fixed[2])
	assert.Equal(t, byte(0), fixed[3])
}

func TestDecodeFixedUint32(t *testing.T) {
	r := rand.Intn(2147483647)
	fixed := EncodeFixedUint32(uint32(r))
	res := DecodeFixedUint32(fixed)
	assert.Equal(t, uint32(r), res)
}

func TestEncodeFixedUint64(t *testing.T) {
	fixed := EncodeFixedUint64(64)
	assert.Equal(t, 8, len(fixed))
	assert.Equal(t, byte(64), fixed[0])
	assert.Equal(t, byte(0), fixed[1])
	assert.Equal(t, byte(0), fixed[2])
	assert.Equal(t, byte(0), fixed[3])
	assert.Equal(t, byte(0), fixed[4])
	assert.Equal(t, byte(0), fixed[5])
	assert.Equal(t, byte(0), fixed[6])
	assert.Equal(t, byte(0), fixed[7])
}

func TestDecodeFixedUint64(t *testing.T) {
	r := rand.Uint64()
	fixed := EncodeFixedUint64(r)
	res := DecodeFixedUint64(fixed)
	assert.Equal(t, r, res)
}
