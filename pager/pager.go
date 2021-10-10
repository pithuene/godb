// Pages are the way the filesystems individual chunks of data are accessed.
// The Pager is responsible for adressing these, mapping them from disk into
// memory when needed, caching these mappings for efficient use and writing
// the modified pages back to disk.
package pager

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type Pager struct {
	Cache Cache
	File  *os.File
}

func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	return &Pager{
		Cache: NewGclockCache(),
		File:  file,
	}, nil
}

func (pager *Pager) mapPageToMemory(pageIdx int64) (*Page, error) {
	fileInfo, err := pager.File.Stat()
	if err != nil {
		return nil, err
	}
	if fileInfo.Size() < PAGE_SIZE*(pageIdx+1)-1 {
		return nil, errors.New("Page idx out of range")
	}
	buffer, err := unix.Mmap(int(pager.File.Fd()), pageIdx*PAGE_SIZE, int(PAGE_SIZE), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	page := &Page{
		Index:  pageIdx,
		Memory: buffer,
	}
	return page, nil
}

// Maps a page into memory if it isn't already and returns it
func (pager *Pager) FetchPage(pageIdx int64) (*Page, error) {
	// Try and get page from cache
	page := pager.Cache.Get(pageIdx)
	if page != nil {
		return page, nil
	}

	// Page not in cache. Map page into memory.
	page, err := pager.mapPageToMemory(pageIdx)
	if err != nil {
		return nil, err
	}

	// Add cache entry
	pager.Cache.Add(page)

	return page, nil
}

func (pager *Pager) AppendPage() (*Page, error) {
	fileInfo, err := pager.File.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	if fileSize%PAGE_SIZE != 0 {
		return nil, errors.New("File size is not a multiple of page size")
	}
	pageCount := fileSize / PAGE_SIZE
	buffer := make([]byte, PAGE_SIZE)
	_, err = pager.File.WriteAt(buffer, fileSize)
	if err != nil {
		return nil, err
	}
	page, err := pager.FetchPage(pageCount)
	return page, err
}

func (pager *Pager) Close() {
	pager.File.Close()
}
