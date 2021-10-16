package table

// Data pages are pages that store actual rows of data.
// A data page always belongs to a single table.
// The data pages of a table form a doubly linked list so the rows can be
// traversed without an index.
// TODO: Data pages keep track of whether they have available space and the ones
// that do form a singly linked list (free list) to make finding a place for
// new data more efficient.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/pager"
)

type DataPageHeader struct {
	// Page indices of adjacent pages
	Next int64
	Prev int64

	// Fixed size of all entries. Includes the header.
	EntrySize int64

	// The number of tuples on this page, which are not in use
	FreeEntries int64
}

// A page used for storing actual data
type DataPage struct {
	// The underlying page
	page   *pager.Page
	Header DataPageHeader
}

func (dataPage *DataPage) Flush() error {
	page := dataPage.encodePage()
	return page.Flush()
}

// Writes back all the header values and returns the underlying raw page
func (page *DataPage) encodePage() *pager.Page {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, page.Header)
	copy(page.page.Memory[0:buf.Len()], buf.Bytes())
	return page.page
}

// The number of entry slots (not necessarily in use)
func (page *DataPage) EntryCapacity() int64 {
	return (pager.PAGE_SIZE - int64(binary.Size(page.Header))) / page.Header.EntrySize
}

func (page *DataPage) GetEntry(entryIdx int64) ([]byte, error) {
	if entryIdx > page.EntryCapacity() {
		return nil, errors.New("Entry index out of range")
	}
	offset := int64(binary.Size(page.Header)) + (entryIdx * page.Header.EntrySize)
	return page.page.Memory[offset : offset+page.Header.EntrySize], nil
}

// Finds a free entry and **marks it as used!**
func (dpage *DataPage) FindFreeEntry() ([]byte, error) {
	offset := int64(binary.Size(dpage.Header))
	for offset+dpage.Header.EntrySize < pager.PAGE_SIZE {
		entry := dpage.page.Memory[offset : offset+dpage.Header.EntrySize]
		entryHeader := DeserializeEntryHeader(entry[0])
		if !entryHeader.InUse {
			entryHeader.InUse = true
			entry[0] = entryHeader.Serialize()
			return entry, nil
		}
		offset += dpage.Header.EntrySize
	}
	return nil, errors.New("No free entry found on page")
}
