package main

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

var PAGE_SIZE = int64(os.Getpagesize())

type RawPage struct {
	// The index of the page in the file
	Index int64
	// The memory memory mapped buffer
	Memory []byte
}

func (page *RawPage) Flush() error {
	return unix.Msync(page.Memory, unix.MS_SYNC)
}

type Pager struct {
	// The pages currently mapped into memory
	Pages map[int64]*RawPage
	File  *os.File
}

func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile("database.db", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	return &Pager{
		Pages: map[int64]*RawPage{},
		File:  file,
	}, nil
}

func (pager *Pager) mapRawPageToMemory(rawPageIdx int64) (*RawPage, error) {
	fileInfo, err := pager.File.Stat()
	if err != nil {
		return nil, err
	}
	if fileInfo.Size() < PAGE_SIZE*(rawPageIdx+1)-1 {
		return nil, errors.New("Raw page idx out of range")
	}
	buffer, err := unix.Mmap(int(pager.File.Fd()), rawPageIdx*PAGE_SIZE, int(PAGE_SIZE), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	page := &RawPage{
		Index:  rawPageIdx,
		Memory: buffer,
	}
	return page, nil
}

// Maps a page into memory if it isn't already and returns it
func (pager *Pager) FetchRawPage(rawPageIdx int64) (*RawPage, error) {
	// Try and get page from cache
	page, isMapped := pager.Pages[rawPageIdx]
	if isMapped {
		return page, nil
	}

	// Page not in cache. Map page into memory.
	page, err := pager.mapRawPageToMemory(rawPageIdx)
	if err != nil {
		return nil, err
	}

	// Add cache entry
	pager.Pages[rawPageIdx] = page
	// TODO: Proper caching strategy. Should "uncache" some other page. See p.117

	return page, nil
}

func (pager *Pager) AppendRawPage() (*RawPage, error) {
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
	page, err := pager.FetchRawPage(pageCount)
	return page, err
}

func (pager *Pager) Close() {
	pager.File.Close()
}
