package id_ttl_ordered_storage

import (
	"fmt"
	"syscall"
)

type MMap struct {
	path   string
	ptr    []byte
	length int
}

func NewMMap(path string, length int) *MMap {
	return &MMap{path: path, length: length}
}

func (m *MMap) ConnectRd() error {
	return m.connect(syscall.O_RDONLY, syscall.PROT_READ)
}

func (m *MMap) ConnectRdWr() error {
	return m.connect(syscall.O_RDWR|syscall.O_CREAT|syscall.O_TRUNC, syscall.PROT_READ|syscall.PROT_WRITE)
}

func (m *MMap) connect(mode, prot int) error {
	fd, err := syscall.Open(m.path, mode, 0o600)
	if err != nil {
		return fmt.Errorf("syscall.Open(%s): %w", m.path, err)
	}
	defer syscall.Close(fd)
	if mode&syscall.O_CREAT != 0 {
		if err := syscall.Ftruncate(fd, int64(m.length)); err != nil {
			return fmt.Errorf("syscall.Ftruncate(%s): %w", m.path, err)
		}
	}

	ptr, err := syscall.Mmap(fd, 0, m.length, prot, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	if err := syscall.Madvise(ptr, syscall.MADV_DONTNEED); err != nil {
		_ = syscall.Munmap(ptr)
		return err
	}
	m.ptr = ptr
	return nil
}

func (m *MMap) Disconnect() error {
	err := syscall.Munmap(m.ptr)
	m.ptr = nil
	return err
}

func (m *MMap) ReadingStopped() error {
	return syscall.Madvise(m.ptr, syscall.MADV_DONTNEED)
}
