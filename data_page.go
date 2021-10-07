package main

// Data pages are pages that store actual rows of data.
// A data page always belongs to a single table.
// The data pages of a table form a doubly linked list so the rows can be
// traversed without an index.
// TODO: Data pages keep track of whether they have available space and the ones
// that do form a singly linked list (free list) to make finding a place for
// new data more efficient.

import (
	"encoding/binary"
	"errors"
	"godb/pager"
)

const DATAPAGE_HEADER_SIZE = 32

// A page used for storing actual data
type DataPage struct {
	// The underlying page
	page *pager.Page

	// Page indices of adjacent pages
	Next int64
	Prev int64

	// Fixed size of all entries. Includes the header.
	EntrySize int64

	// The number of tuples on this page, which are not in use
	FreeEntries int64
}

func (dataPage *DataPage) Flush() error {
	page := dataPage.encodePage()
	return page.Flush()
}

// Writes back all the header values and returns the underlying raw page
func (page *DataPage) encodePage() *pager.Page {
	binary.PutVarint(page.page.Memory[0:8], page.Next)
	binary.PutVarint(page.page.Memory[8:16], page.Prev)
	binary.PutVarint(page.page.Memory[16:24], page.EntrySize)
	binary.PutVarint(page.page.Memory[24:32], page.FreeEntries)
	return page.page
}

// The number of entry slots (not necessarily in use)
func (page *DataPage) EntryCapacity() int64 {
	return (pager.PAGE_SIZE - DATAPAGE_HEADER_SIZE) / ENTRY_SIZE
}

func (page *DataPage) GetEntry(entryIdx int64) ([]byte, error) {
	if entryIdx > page.EntryCapacity() {
		return nil, errors.New("Entry index out of range")
	}
	offset := DATAPAGE_HEADER_SIZE + (entryIdx * ENTRY_SIZE)
	return page.page.Memory[offset : offset+ENTRY_SIZE], nil
}

// Finds a free entry and **marks it as used!**
func (dpage *DataPage) FindFreeEntry() ([]byte, error) {
	offset := int64(DATAPAGE_HEADER_SIZE)
	for offset+ENTRY_SIZE < pager.PAGE_SIZE {
		entry := dpage.page.Memory[offset : offset+ENTRY_SIZE]
		entryHeader := DeserializeEntryHeader(entry[0])
		if !entryHeader.InUse {
			entryHeader.InUse = true
			entry[0] = entryHeader.Serialize()
			return entry, nil
		}
		offset += ENTRY_SIZE
	}
	return nil, errors.New("No free entry found on page")
}
