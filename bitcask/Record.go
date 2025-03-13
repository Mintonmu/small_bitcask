package bitcask

import "encoding/binary"

type Record struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

const entryHeaderSize = 10

const (
	PUT uint16 = iota
	DEL
)

func (r *Record) GetSize() int64 {
	return int64(entryHeaderSize + r.KeySize + r.ValueSize)
}

func (r *Record) Encode() ([]byte, error) {
	buf := make([]byte, r.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], r.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], r.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], r.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+r.KeySize], r.Key)
	copy(buf[entryHeaderSize+r.KeySize:], r.Value)
	return buf, nil
}

func NewRecord(key, value []byte, mark uint16) *Record {
	return &Record{
		Key:       key,
		Value:     value,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      mark,
	}
}

func Decode(buf []byte) (*Record, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	return &Record{KeySize: ks, ValueSize: vs, Mark: mark}, nil
}
