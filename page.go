package main

// Pages are the way the filesystems individual chunks of data are accessed.
// The Pager is responsible for adressing these, mapping them from disk into
// memory when needed, caching these mappings for efficient use and writing
// the modified pages back to disk.

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

var PAGE_SIZE = int64(os.Getpagesize())

type Page struct {
	// The index of the page in the file
	Index int64
	// The memory memory mapped buffer
	Memory []byte
}

func (page *Page) Flush() error {
	return unix.Msync(page.Memory, unix.MS_SYNC)
}

type Pager struct {
	// The pages currently mapped into memory
	Pages map[int64]*Page
	File  *os.File
}

func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile("database.db", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	return &Pager{
		Pages: map[int64]*Page{},
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
	page, isMapped := pager.Pages[pageIdx]
	if isMapped {
		return page, nil
	}

	// Page not in cache. Map page into memory.
	page, err := pager.mapPageToMemory(pageIdx)
	if err != nil {
		return nil, err
	}

	// Add cache entry
	pager.Pages[pageIdx] = page
	// TODO: Proper caching strategy. Should "uncache" some other page. See p.117

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
