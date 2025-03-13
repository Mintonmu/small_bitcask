package bitcask

import (
	"os"
	"path/filepath"
	"sync"
)

const (
	BitCaskFileName = "minibitcask.data"
	MergeFileName   = "minibitcask.data.merge"
)

type DBFile struct {
	file          *os.File
	Offset        int64
	HeaderBufPool *sync.Pool
}

func CreateFile(path string) (*DBFile, error) {
	return NewDBFile(filepath.Join(path, BitCaskFileName))
}

func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	pool := &sync.Pool{New: func() interface{} {
		return make([]byte, entryHeaderSize)
	}}
	return &DBFile{Offset: stat.Size(), file: file, HeaderBufPool: pool}, nil
}

func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return newInternal(fileName)
}

func NewDBFile(filepath string) (*DBFile, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &DBFile{
		file:   file,
		Offset: stat.Size(),
		HeaderBufPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, entryHeaderSize)
			},
		},
	}, nil
}

func (p *DBFile) Read(off int64) (r *Record, err error) {

	buf := p.HeaderBufPool.Get().(*[]byte)
	defer p.HeaderBufPool.Put(buf)
	if _, err := p.file.ReadAt(*buf, off); err != nil {
		return nil, err
	}
	off += entryHeaderSize
	if r.KeySize > 0 {
		key := make([]byte, r.KeySize)
		if _, err := p.file.ReadAt(key, off); err != nil {
			return nil, err
		}
		r.Key = key
	}
	off += int64(r.KeySize)

	if r.ValueSize > 0 {
		value := make([]byte, r.ValueSize)
		if _, err := p.file.ReadAt(value, off); err != nil {
			return nil, err
		}
		r.Value = value
	}
	return

}

func (p *DBFile) Write(r *Record) (err error) {
	enc, err := r.Encode()
	if err != nil {
		return err
	}
	_, err = p.file.WriteAt(enc, p.Offset)
	p.Offset += r.GetSize()
	return
}
