package main

import (
	"encoding/binary"
	"errors"
)

type Table struct {
	database *Database
	// The index of the first DataPage
	FirstPageIdx int64
	LastPageIdx  int64
}

func (table *Table) NewDataPage() (*DataPage, error) {
	page, err := table.database.Pager.AppendPage()
	if err != nil {
		return nil, err
	}
	dataPage := &DataPage{
		page:      page,
		Next:      -1,
		Prev:      table.LastPageIdx,
		EntrySize: ENTRY_SIZE,
	}
	dataPage.FreeEntries = dataPage.EntryCapacity()
	return dataPage, nil
}

func (table *Table) FetchDataPage(pageIdx int64) (*DataPage, error) {
	page, err := table.database.Pager.FetchPage(pageIdx)
	if err != nil {
		return nil, err
	}

	dataPage, err := page.decodePage()
	if err != nil {
		return nil, err
	}

	return dataPage, nil
}

// TODO: This only makes sense as long as all data is (int64, int64)
const ENTRY_SIZE = 17

// Find page which still has free entries and create one if there is none.
// TODO: Currently iterates through all pages. This should be handled using a free list or the like.
func (table *Table) FindFreePage() (*DataPage, error) {
	pageIdx := int64(0)
	for {
		dpage, err := table.FetchDataPage(pageIdx)
		if err != nil { // Error occurs when pageIdx is out of range, so there is no free space on any page
			dpage, err = table.NewDataPage()
			if err != nil {
				return nil, err
			}
			return dpage, nil
		}
		if dpage.FreeEntries > 0 {
			return dpage, nil
		} else {
			pageIdx++
		}
	}
}

// TODO: Every tuple is going to be (int64, int64) for now
func (table *Table) Insert(key int64, value int64) error {
	page, err := table.FindFreePage()

	entryBuffer, err := page.FindFreeEntry()
	if err != nil {
		return err
	}

	binary.PutVarint(entryBuffer[1:9], key)
	binary.PutVarint(entryBuffer[9:17], value)

	page.Flush()

	return nil
}

// TODO: This only makes sense as long as all data is (int64, int64)
func (table *Table) Select(key int64) (int64, error) {
	pageIdx := int64(0)
	for {
		page, err := table.FetchDataPage(pageIdx)
		if err != nil { // pageIdx out of range. Key not found.
			return -1, errors.New("Key not found")
		}

		for entryIdx := int64(0); entryIdx < page.EntryCapacity(); entryIdx++ {
			entryBuffer, err := page.GetEntry(entryIdx)
			if err != nil {
				return -1, err
			}
			entryHeader := DeserializeEntryHeader(entryBuffer[0])
			if entryHeader.InUse {
				entryKey, n := binary.Varint(entryBuffer[1:9])
				if n <= 0 {
					return -1, err
				}
				if entryKey == key {
					// FOUND!
					value, n := binary.Varint(entryBuffer[9:17])
					if n <= 0 {
						return -1, err
					}
					return value, nil
				}
			}
		}
		pageIdx++
	}
}

func (table *Table) NextPage(currPage *DataPage) (*DataPage, error) {
	if currPage.Next < 0 {
		return nil, errors.New("There is no next page, this is the last one")
	}
	nextPage, err := table.FetchDataPage(currPage.Next)
	return nextPage, err
}

func (table *Table) PreviousPage(currPage *DataPage) (*DataPage, error) {
	if currPage.Prev < 0 {
		return nil, errors.New("There is no previous page, this is the first one")
	}
	previousPage, err := table.FetchDataPage(currPage.Prev)
	return previousPage, err
}