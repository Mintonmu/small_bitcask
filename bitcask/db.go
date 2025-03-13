package bitcask

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type MiniBitcask struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile  *DBFile          // 数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}

func Open(dirPath string) (db *MiniBitcask, err error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dirAbsPath, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}
	dbFile, err := NewDBFile(dirAbsPath)
	if err != nil {
		return nil, err
	}
	db = &MiniBitcask{
		indexes: make(map[string]int64),
		dbFile:  dbFile,
		dirPath: dirAbsPath,
	}

	// 从数据文件中加载索引信息
	if err := db.loadIndexes(); err != nil {
		return nil, err
	}

	return
}

func (m *MiniBitcask) loadIndexes() error {
	if m.dbFile == nil {
		return ErrInvalidDBFile
	}
	var offset int64
	for {
		record, err := m.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		m.indexes[string(record.Key)] = offset
		if record.Mark == DEL {
			// 删除内存中的 key
			delete(m.indexes, string(record.Key))
		}
		offset += record.GetSize()
	}
	return nil
}

// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (db *MiniBitcask) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Record
		offset       int64
	)

	// 读取原数据文件中的 Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	// 新建临时文件
	mergeDBFile, err := NewMergeDBFile(db.dirPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(mergeDBFile.file.Name())
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	// 重新写入有效的 entry
	for _, entry := range validEntries {
		writeOff := mergeDBFile.Offset
		err = mergeDBFile.Write(entry)
		if err != nil {
			return err
		}

		// 更新索引
		db.indexes[string(entry.Key)] = writeOff
	}

	// 获取文件名
	dbFileName := db.dbFile.file.Name()
	// 关闭文件
	_ = db.dbFile.file.Close()
	// 删除旧的数据文件
	_ = os.Remove(dbFileName)
	_ = mergeDBFile.file.Close()
	// 获取文件名
	mergeDBFileName := mergeDBFile.file.Name()
	// 临时文件变更为新的数据文件
	_ = os.Rename(mergeDBFileName, filepath.Join(db.dirPath, BitCaskFileName))

	dbFile, err := NewDBFile(db.dirPath)
	if err != nil {
		return err
	}

	db.dbFile = dbFile
	return nil
}

func (db *MiniBitcask) Put(key, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	// 封装成 Entry
	entry := NewRecord(key, value, PUT)
	// 追加到数据文件当中
	err = db.dbFile.Write(entry)

	// 写到内存
	db.indexes[string(key)] = offset
	return
}

func (m *MiniBitcask) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	offset, err := m.exist(key)
	if err == ErrKeyNotFound {
		return
	}

	// 从磁盘中读取数据
	var e *Record
	e, err = m.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		value = e.Value
	}
	return
}

func (db *MiniBitcask) Delete(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_, err = db.exist(key)
	if err == ErrKeyNotFound {
		err = nil
		return
	}

	// 封装成 Entry 并写入
	e := NewRecord(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的 key
	delete(db.indexes, string(key))
	return
}

func (db *MiniBitcask) exist(key []byte) (int64, error) {
	// 从内存当中取出索引信息
	offset, ok := db.indexes[string(key)]
	// key 不存在
	if !ok {
		return 0, ErrKeyNotFound
	}
	return offset, nil
}

func (m *MiniBitcask) Close() error {
	if m.dbFile != nil {
		return m.dbFile.file.Close()
	}
	return ErrInvalidDBFile
}
