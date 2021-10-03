package main

import (
	"encoding/binary"
	"errors"
)

type Database struct {
	Pager *Pager
}

func OpenDatabase(filename string) (*Database, error) {
	rawPager, err := OpenRawPager(filename)
	if err != nil {
		return nil, err
	}
	return &Database{
		Pager: &Pager{
			RawPager: rawPager,
		},
	}, nil
}

func (db *Database) Close() {
	db.Pager.Close()
}

type EntryHeader struct {
	InUse bool
}

const EntryHeaderMaskInUse = 0b00000001

func (eh *EntryHeader) Serialize() byte {
	var res byte
	if eh.InUse {
		res |= EntryHeaderMaskInUse
	}
	return res
}

func DeserializeEntryHeader(serialized byte) *EntryHeader {
	var eh EntryHeader
	if serialized&EntryHeaderMaskInUse != 0 {
		eh.InUse = true
	}
	return &eh
}

// TODO: This only makes sense as long as all data is (int64, int64)
const ENTRY_SIZE = 17

// Find page which still has free entries and create one if there is none.
// TODO: Currently iterates through all pages. This should be handled using a free list or the like.
func (db *Database) FindFreePage() (*Page, error) {
	pageIdx := int64(0)
	for {
		page, err := db.Pager.Fetch(pageIdx)
		if err != nil { // Error occurs when pageIdx is out of range, so there is no free space on any page
			page, err = db.Pager.AddPage()
			if err != nil {
				return nil, err
			}
			return page, nil
		}
		if page.FreeEntries > 0 {
			return page, nil
		} else {
			pageIdx++
		}
	}
}

// TODO: Every tuple is going to be (int64, int64) for now
func (db *Database) Insert(key int64, value int64) error {
	page, err := db.FindFreePage()

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
func (db *Database) Select(key int64) (int64, error) {
	pageIdx := int64(0)
	for {
		page, err := db.Pager.Fetch(pageIdx)
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
