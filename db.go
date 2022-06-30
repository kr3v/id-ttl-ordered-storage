package id_ttl_ordered_storage

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	ErrPathNotFound = errors.New("")
)

///

type storedPath struct {
	from, until time.Time
	mmap        *MMap

	bytes int
	count int
}

///

type ID struct {
	pathIdx int
	offset  int64
	length  int
}

///

type Options struct {
	MaxBufferSize int
	TTL           time.Duration
	Path          string
}

///

type DB struct {
	options Options
	mutex   sync.Mutex

	counter int
	current *storedPath
	paths   map[int]*storedPath
}

func NewDB(options Options) (*DB, error) {
	if err := os.RemoveAll(options.Path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(options.Path, 0o700); err != nil {
		return nil, err
	}
	db := &DB{options: options, paths: make(map[int]*storedPath)}
	if err := db.bufferInit(); err != nil {
		return nil, err
	}

	//go func() {
	//	t := time.NewTicker(options.TTL)
	//	defer t.Stop()
	//	for {
	//		select {
	//		case <-t.C:
	//
	//		}
	//	}
	//}()

	return db, nil
}

func (db *DB) Close() (err1 error) {
	for _, path := range db.paths {
		if err := path.mmap.Disconnect(); err != nil && err1 == nil {
			err1 = err
		}
	}
	return err1
}

///

type ids []ID

func (a ids) Len() int {
	return len(a)
}

func (a ids) Less(i, j int) bool {
	v1 := a[i]
	v2 := a[j]
	return (v1.pathIdx < v2.pathIdx) || v1.pathIdx == v2.pathIdx && v1.offset < v2.offset
}

func (a ids) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (db *DB) GetManyMMapB(vs []ID, dsts [][]byte) (vals [][]byte, err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var (
		prev ID
		mmap *MMap
	)
	defer func() {
		if mmap != nil {
			if err := mmap.ReadingStopped(); err != nil {
				log.Println(err)
			}
		}
	}()

	if len(dsts) < len(vs) {
		dsts = append(dsts, make([][]byte, len(vs)-len(dsts))...)
	}

	sort.Sort(ids(vs))
	for i, id := range vs {
		if prev.length == 0 || prev.pathIdx != id.pathIdx {
			if mmap != nil {
				if err := mmap.ReadingStopped(); err != nil {
					return nil, fmt.Errorf("%v.ReadingStopped: %w", mmap.path, err)
				}
			}
			mmap = nil
		}
		prev = id

		dst := append(dsts[i][:0], make([]byte, id.length)...)
		path, ok := db.paths[id.pathIdx]
		if !ok {
			return nil, ErrPathNotFound
		}
		copy(dst, path.mmap.ptr[id.offset:int(id.offset)+id.length])
		dsts[i] = dst
	}
	return dsts, nil
}

///

func (db *DB) Put(val []byte) (ID, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.current.bytes+len(val) > db.options.MaxBufferSize {
		if err := db.bufferFlush(); err != nil {
			return ID{}, err
		}
		if err := db.bufferInit(); err != nil {
			return ID{}, err
		}
	}
	offset := db.current.bytes
	offsetB := db.current.mmap.ptr[offset:]
	copy(offsetB, val)
	db.current.bytes += len(val)
	db.current.count++
	return ID{pathIdx: db.counter, offset: int64(offset), length: len(val)}, nil
}

func (db *DB) nextPath() string {
	db.counter += 1
	return db.path(db.counter)
}

func (db *DB) path(idx int) string {
	return db.pathB(idx, new(StringBuilderUnsafe))
}

func (db *DB) pathB(idx int, b *StringBuilderUnsafe) string {
	b.Reset()
	b.WriteString(db.options.Path)
	b.WriteString("/")
	b.WriteString(strconv.Itoa(idx))
	b.WriteString(".seq")
	return b.String()
}

func (db *DB) bufferFlush() error {
	err := db.current.mmap.Disconnect()
	if err != nil {
		return err
	}
	return db.current.mmap.ConnectRd()
}

func (db *DB) bufferInit() error {
	p := db.nextPath()
	m := NewMMap(p, db.options.MaxBufferSize)
	if err := m.ConnectRdWr(); err != nil {
		return fmt.Errorf("m.ConnectRdWr(%s): %w", p, err)
	}
	db.current = &storedPath{from: time.Now(), mmap: m}
	db.paths[db.counter] = db.current
	return nil
}
