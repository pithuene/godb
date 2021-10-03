package main

import (
	"encoding/binary"
	"errors"
)

const PAGE_HEADER_SIZE = 32

type Page struct {
	// The underlying raw page
	rawPage *RawPage

	// TODO: Not actually used yet
	// Raw page indices of adjacent pages
	Next int64
	Prev int64

	// Fixed size of all entries. Includes the header.
	EntrySize int64

	// The number of tuples on this page, which are not in use
	FreeEntries int64
}

type Pager struct {
	RawPager *RawPager
}

func (pager *Pager) Close() {
	pager.RawPager.Close()
}

func (pager *Pager) AddPage() (*Page, error) {
	rawPage, err := pager.RawPager.AppendRawPage()
	if err != nil {
		return nil, err
	}
	page := &Page{
		rawPage:   rawPage,
		Next:      0, // TODO
		Prev:      0, // TODO
		EntrySize: ENTRY_SIZE,
	}
	page.FreeEntries = page.EntryCapacity()
	return page, nil
}

func (pager *Pager) Fetch(pageIdx int64) (*Page, error) {
	rawPage, err := pager.RawPager.FetchRawPage(pageIdx)
	if err != nil {
		return nil, err
	}

	page, err := rawPage.decodeRawPage()
	if err != nil {
		return nil, err
	}

	return page, nil
}

func (page *Page) Flush() error {
	rawPage := page.encodePage()
	return rawPage.Flush()
}

// Reads the header of a RawPage and returns the resulting Page
func (rawPage *RawPage) decodeRawPage() (*Page, error) {
	page := &Page{
		rawPage: rawPage,
	}

	next, n := binary.Varint(rawPage.Memory[0:8])
	if n <= 0 {
		return nil, errors.New("Next deserialization failed")
	}
	page.Next = next

	prev, n := binary.Varint(rawPage.Memory[8:16])
	if n <= 0 {
		return nil, errors.New("Prev deserialization failed")
	}
	page.Prev = prev

	entrySize, n := binary.Varint(rawPage.Memory[16:24])
	if n <= 0 {
		return nil, errors.New("EntrySize deserialization failed")
	}
	page.EntrySize = entrySize

	freeEntries, n := binary.Varint(rawPage.Memory[24:32])
	if n <= 0 {
		return nil, errors.New("FreeEntries deserialization failed")
	}
	page.FreeEntries = freeEntries

	return page, nil
}

// Writes back all the header values and returns the underlying raw page
func (page *Page) encodePage() *RawPage {
	binary.PutVarint(page.rawPage.Memory[0:8], page.Next)
	binary.PutVarint(page.rawPage.Memory[8:16], page.Prev)
	binary.PutVarint(page.rawPage.Memory[16:24], page.EntrySize)
	binary.PutVarint(page.rawPage.Memory[24:32], page.FreeEntries)
	return page.rawPage
}

// The number of entry slots (not necessarily in use)
func (page *Page) EntryCapacity() int64 {
	return (PAGE_SIZE - PAGE_HEADER_SIZE) / ENTRY_SIZE
}

func (page *Page) GetEntry(entryIdx int64) ([]byte, error) {
	if entryIdx > page.EntryCapacity() {
		return nil, errors.New("Entry index out of range")
	}
	offset := PAGE_HEADER_SIZE + (entryIdx * ENTRY_SIZE)
	return page.rawPage.Memory[offset : offset+ENTRY_SIZE], nil
}

// Finds a free entry and **marks it as used!**
func (page *Page) FindFreeEntry() ([]byte, error) {
	offset := int64(PAGE_HEADER_SIZE)
	for offset+ENTRY_SIZE < PAGE_SIZE {
		entry := page.rawPage.Memory[offset : offset+ENTRY_SIZE]
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
