package id_ttl_ordered_storage

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

///

type buffer struct {
	buff    []byte
	counter int

	from time.Time
	db   *DB
	path string
}

func newBuffer(db *DB, path string, prevBuff []byte) buffer {
	return buffer{path: path, db: db, from: time.Now(), buff: prevBuff}
}

///

type storedPath struct {
	from, until time.Time
	records     int
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

	buffer buffer

	currentPath string
	pathCounter int
	paths       []storedPath
}

func NewDB(options Options) (*DB, error) {
	if err := os.RemoveAll(options.Path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(options.Path, 0o700); err != nil {
		return nil, err
	}
	db := &DB{options: options}
	db.currentPath = db.updatePath()
	db.buffer = newBuffer(db, db.currentPath, make([]byte, 0, options.MaxBufferSize))
	return db, nil
}

///

func (db *DB) Get(id ID) (val []byte, err error) {
	return db.GetB(id, make([]byte, id.length))
}

func (db *DB) GetB(id ID, dst []byte) (val []byte, err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	dst = dst[:0]
	dst = append(dst, make([]byte, id.length)...)

	p := db.path(id.pathIdx)
	if db.pathCounter == id.pathIdx {
		copy(dst, db.buffer.buff[id.offset:int(id.offset)+id.length])
		return dst, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	seek, err := f.Seek(id.offset, 0)
	if err != nil {
		return nil, err
	}
	if seek != id.offset {
		return nil, errors.New("seek != id.offset")
	}
	_, err = io.ReadAtLeast(f, dst, id.length)
	if err != nil {
		return nil, err
	}
	return dst, nil
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

func (db *DB) GetMany(vs []ID) (vals [][]byte, err error) {
	return db.GetManyB(vs, nil, &StringBuilderUnsafe{})
}

func (db *DB) GetManyB(vs []ID, dsts [][]byte, pathBuilder *StringBuilderUnsafe) (vals [][]byte, err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var (
		prevID ID
		file   *os.File
	)
	defer func() {
		if file != nil {
			_ = file.Close()
			file = nil
		}
	}()

	if len(dsts) < len(vs) {
		dsts = append(dsts, make([][]byte, len(vs)-len(dsts))...)
	}

	sort.Sort(ids(vs))
	for i, id := range vs {
		if prevID.length == 0 || prevID.pathIdx != id.pathIdx {
			if file != nil {
				_ = file.Close()
			}
			file = nil
		}
		prevID = id

		dst := append(dsts[i][:0], make([]byte, id.length)...)
		if db.pathCounter == id.pathIdx {
			copy(dst, db.buffer.buff[id.offset:int(id.offset)+id.length])
		} else {
			if file == nil {
				file, err = os.Open(db.pathB(id.pathIdx, pathBuilder))
				if err != nil {
					return nil, err
				}
			}
			if seek, err := file.Seek(id.offset, 0); err != nil {
				return nil, err
			} else if seek != id.offset {
				return nil, errors.New("seek != id.offset")
			}
			_, err = io.ReadAtLeast(file, dst, id.length)
			if err != nil {
				return nil, err
			}
		}
		dsts[i] = dst
	}
	return dsts, nil
}

///

func (db *DB) Put(val []byte) (ID, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if len(db.buffer.buff)+len(val) > db.options.MaxBufferSize {
		if err := db.flush(); err != nil {
			return ID{}, err
		}
	}
	offset := len(db.buffer.buff)
	db.buffer.buff = append(db.buffer.buff, val...)
	return ID{pathIdx: db.pathCounter, offset: int64(offset), length: len(val)}, nil
}

func (db *DB) updatePath() string {
	db.pathCounter += 1
	return db.path(db.pathCounter)
}

func (db *DB) path(idx int) string {
	return path.Join(db.options.Path, strconv.Itoa(idx)+".seq")
}
func (db *DB) pathB(idx int, b *StringBuilderUnsafe) string {
	b.Reset()
	b.WriteString(db.options.Path)
	b.WriteString("/")
	b.WriteString(strconv.Itoa(idx))
	b.WriteString(".seq")
	return b.String()
}

func (db *DB) flush() error {
	if err := ioutil.WriteFile(db.buffer.path, db.buffer.buff, 0o600); err != nil {
		return err
	}
	db.paths = append(db.paths, storedPath{
		from:    db.buffer.from,
		until:   time.Now(),
		records: db.buffer.counter,
	})
	db.currentPath = db.updatePath()
	db.buffer = newBuffer(db, db.currentPath, db.buffer.buff[:0])
	return nil
}
