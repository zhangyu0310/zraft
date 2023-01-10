package zdb

type VarUint64 []byte

func EncodeVarUint64(n uint64) VarUint64 {
	res := make([]byte, 0, 1)
	for n >= 128 {
		res = append(res, byte(n|128))
		n >>= 7
	}
	res = append(res, byte(n))
	return res
}

func DecodeVarUint64(n VarUint64) uint64 {
	var res uint64
	index := 0
	for shift := 0; shift <= 63; shift += 7 {
		b := n[index]
		index++
		if (b & 128) != 0 {
			res |= uint64(b) & 127 << shift
		} else {
			res |= uint64(b) << shift
			break
		}
	}
	return res
}

func GetVarUint64(data []byte, index uint32) (VarUint64, uint32) {
	varUint64 := make(VarUint64, 0, 4)
	for {
		varUint64 = append(varUint64, data[index])
		if data[index]&128 == 0 {
			index++
			break
		}
		index++
	}
	return varUint64, index
}

func GetLengthAndValue(data []byte, index uint32) (uint32, []byte, uint32) {
	var lenVar VarUint64
	lenVar, index = GetVarUint64(data, index)
	resLen := DecodeVarUint64(lenVar)
	result := data[index : index+uint32(resLen)]
	index += uint32(resLen)
	return uint32(resLen), result, index
}

type FixedUint16 [2]byte

func EncodeFixedUint16(n uint16) FixedUint16 {
	res := new(FixedUint16)
	res[0] = byte(n)
	res[1] = byte(n >> 8)
	return *res
}

func DecodeFixedUint16(n FixedUint16) uint16 {
	res := uint16(n[0]) + (uint16(n[1]) << 8)
	return res
}

func GetFixedUint16(data []byte, index int) FixedUint16 {
	res := new(FixedUint16)
	res[0] = data[index]
	res[1] = data[index+1]
	return *res
}

type FixedUint32 [4]byte

func EncodeFixedUint32(n uint32) FixedUint32 {
	res := new(FixedUint32)
	res[0] = byte(n)
	res[1] = byte(n >> 8)
	res[2] = byte(n >> 16)
	res[3] = byte(n >> 24)
	return *res
}

func DecodeFixedUint32(n FixedUint32) uint32 {
	res := uint32(n[0]) + (uint32(n[1]) << 8) + (uint32(n[2]) << 16) + (uint32(n[3]) << 24)
	return res
}

func GetFixedUint32(data []byte, index int) FixedUint32 {
	res := new(FixedUint32)
	res[0] = data[index]
	res[1] = data[index+1]
	res[2] = data[index+2]
	res[3] = data[index+3]
	return *res
}

type FixedUint64 [8]byte

func EncodeFixedUint64(n uint64) FixedUint64 {
	res := new(FixedUint64)
	res[0] = byte(n)
	res[1] = byte(n >> 8)
	res[2] = byte(n >> 16)
	res[3] = byte(n >> 24)
	res[4] = byte(n >> 32)
	res[5] = byte(n >> 40)
	res[6] = byte(n >> 48)
	res[7] = byte(n >> 56)
	return *res
}

func DecodeFixedUint64(n FixedUint64) uint64 {
	res := uint64(n[0]) + (uint64(n[1]) << 8) +
		(uint64(n[2]) << 16) + (uint64(n[3]) << 24) +
		(uint64(n[4]) << 32) + (uint64(n[5]) << 40) +
		(uint64(n[6]) << 48) + (uint64(n[7]) << 56)
	return res
}

func GetFixedUint64(data []byte, index int) FixedUint64 {
	res := new(FixedUint64)
	res[0] = data[index]
	res[1] = data[index+1]
	res[2] = data[index+2]
	res[3] = data[index+3]
	res[4] = data[index+4]
	res[5] = data[index+5]
	res[6] = data[index+6]
	res[7] = data[index+7]
	return *res
}
