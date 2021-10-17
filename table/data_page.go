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

// The ratio of space unavailable to inserts.
// This space is kept free to allow for resizing of variable length entries without moving them to another page.
const DATA_PAGE_FREE_SPACE_RATIO = 0.2

var DataPageFreeSpace = int16(float32(pager.PAGE_SIZE) * DATA_PAGE_FREE_SPACE_RATIO)

type DataPageHeader struct {
	// Page indices of adjacent pages
	Next int64
	Prev int64

	RowPointersLength int16

	// The offset of the first free byte.
	// Increases as more RowPointers are added.
	FreeSpaceStart int16
	// The offset of the last non-free byte.
	// Increases as more row data is added at the end.
	FreeSpaceEnd int16
}

// A page used for storing actual data
type DataPage struct {
	// The underlying page
	page *pager.Page
	// Fixed length header
	Header DataPageHeader
	// The offsets of the rows on this page.
	// Negative if not in use.
	RowPointers []int16
}

func (dataPage *DataPage) Flush() error {
	page := dataPage.encodePage()
	return page.Flush()
}

// Writes back all the header values including the row pointers and returns the underlying raw page
func (page *DataPage) encodePage() *pager.Page {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, page.Header)
	copy(page.page.Memory[0:buf.Len()], buf.Bytes())

	buf.Reset()
	binary.Write(&buf, binary.BigEndian, page.RowPointers)
	bytes := buf.Bytes()
	copy(page.page.Memory[binary.Size(page.Header):page.Header.FreeSpaceStart], bytes)

	return page.page
}

// Returns a byte slice with beginning with the entry at the given index.
// The slice will extend to the end of the page as entry length isn't known.
func (page *DataPage) GetEntry(entryIdx int16) ([]byte, error) {
	if entryIdx >= page.Header.RowPointersLength {
		return nil, errors.New("Entry index out of range")
	}

	offset := page.RowPointers[entryIdx]
	if offset < 0 { // Pointer not in use
		return nil, errors.New("Entry index empty")
	}

	return page.page.Memory[offset:], nil
}

// Finds or creates an available RowPointer
func (dpage *DataPage) findAvailableRowPointer() int {
	for i := int16(0); i < dpage.Header.RowPointersLength; i++ {
		rowPointer := dpage.RowPointers[i]
		if rowPointer < 0 { // Available
			return int(i)
		}
	}

	// Add new row pointer
	dpage.RowPointers = append(dpage.RowPointers, -1)
	// Adjust FreeSpaceStart
	dpage.Header.FreeSpaceStart += 4
	dpage.Header.RowPointersLength++
	return len(dpage.RowPointers) - 1
}

// Checks how much space is available for insert
func (dpage *DataPage) AvailableSpace() int {
	freeSpace := dpage.Header.FreeSpaceEnd - dpage.Header.FreeSpaceStart
	insertAvailableSpace := freeSpace - DataPageFreeSpace
	return int(insertAvailableSpace)
}

// Finds a free entry and **marks it as used!**
// Receives the length required for the new entry.
func (dpage *DataPage) FindFreeEntry(requiredSpace int) ([]byte, error) {
	if dpage.AvailableSpace() < requiredSpace {
		return nil, errors.New("No available space left on page")
	}

	rowPointerIdx := dpage.findAvailableRowPointer()
	rowStart := dpage.Header.FreeSpaceEnd - int16(requiredSpace) - 1
	dpage.RowPointers[rowPointerIdx] = rowStart
	dpage.Header.FreeSpaceEnd = rowStart

	return dpage.page.Memory[rowStart : int(rowStart)+requiredSpace], nil
}
