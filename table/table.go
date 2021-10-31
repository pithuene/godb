package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/pager"
)

type Table struct {
	Name string
	// Pointer to the database pager
	Pager *pager.Pager
	// The index of the first DataPage
	FirstPageIdx int64
	LastPageIdx  int64
	Schema       TableSchema
}

func (table *Table) NewDataPage() (*DataPage, error) {
	page, err := table.Pager.AppendPage()
	if err != nil {
		return nil, err
	}

	if table.LastPageIdx >= 0 {
		prevPage, err := table.FetchDataPage(table.LastPageIdx)
		if err != nil {
			return nil, err
		}
		prevPage.Header.Next = page.Index
		prevPage.Flush()
	} else {
		table.FirstPageIdx = page.Index
	}
	table.LastPageIdx = page.Index

	dataPage := &DataPage{
		page: page,
		Header: DataPageHeader{
			Next:           -1,
			Prev:           table.LastPageIdx,
			FreeSpaceStart: int16(binary.Size(DataPageHeader{})),
			FreeSpaceEnd:   int16(pager.PAGE_SIZE),
		},
	}
	return dataPage, nil
}

func (table *Table) FetchDataPage(pageIdx int64) (*DataPage, error) {
	page, err := table.Pager.FetchPage(pageIdx)
	if err != nil {
		return nil, err
	}

	dataPage, err := table.decodePage(page)
	if err != nil {
		return nil, err
	}

	return dataPage, nil
}

// Find page which still has sufficient space and create one if there is none.
// TODO: Currently iterates through all pages. This should be handled using a free list or the like.
func (table *Table) FindFreePage(requiredSpace int) (*DataPage, error) {
	dpage, err := table.FetchDataPage(table.FirstPageIdx)

	for {
		if err != nil { // Error occurs when pageIdx is out of range, so there is no free space on any page
			newPage, err := table.NewDataPage()
			if err != nil {
				return nil, err
			}

			// Update table
			table.LastPageIdx = newPage.page.Index
			if table.FirstPageIdx < 0 {
				table.FirstPageIdx = table.LastPageIdx
			}

			return newPage, nil
		}
		if dpage.AvailableSpace() >= requiredSpace {
			return dpage, nil
		} else {
			dpage, err = table.NextPage(dpage)
		}
	}
}

func (row Row) Encode(targetBuffer []byte) {
	offset := 0
	for _, column := range row {
		// TODO: There is probably a better way to insert a slice into another one
		encoded := column.Encode()
		for i := 0; i < int(column.Length()); i++ {
			targetBuffer[offset+i] = encoded[i]
		}
		offset += int(column.Length())
	}
}

// The given buffer starts with the row value, but can be longer.
// This is necessary, because the length of the value is not yet known.
//
// Returns the row, the number of bytes read and optionally an error.
func (table *Table) DecodeRow(buffer []byte, schema TableSchema) (Row, int64, error) {
	row := make([]ColumnValue, len(table.Schema.Columns))
	offset := int64(0)
	for i, colDef := range table.Schema.Columns {
		val, err := colDef.Type.Decode(buffer[offset:])
		if err != nil {
			return nil, -1, err
		}

		row[i] = val

		offset += int64(val.Length())
	}
	return row, offset, nil
}

func (table *Table) NextPage(currPage *DataPage) (*DataPage, error) {
	if currPage.Header.Next < 0 {
		return nil, errors.New("There is no next page, this is the last one")
	}
	nextPage, err := table.FetchDataPage(currPage.Header.Next)
	return nextPage, err
}

func (table *Table) PreviousPage(currPage *DataPage) (*DataPage, error) {
	if currPage.Header.Prev < 0 {
		return nil, errors.New("There is no previous page, this is the first one")
	}
	previousPage, err := table.FetchDataPage(currPage.Header.Prev)
	return previousPage, err
}

// Reads the header of a Page and returns the resulting DataPage
func (table *Table) decodePage(page *pager.Page) (*DataPage, error) {
	dataPage := &DataPage{
		page:   page,
		Header: DataPageHeader{},
	}

	reader := bytes.NewReader(page.Memory[0:binary.Size(dataPage.Header)])
	binary.Read(reader, binary.BigEndian, &dataPage.Header)

	dataPage.RowPointers = make([]int16, dataPage.Header.RowPointersLength)

	reader = bytes.NewReader(page.Memory[binary.Size(dataPage.Header):dataPage.Header.FreeSpaceStart])
	binary.Read(reader, binary.BigEndian, &dataPage.RowPointers)

	return dataPage, nil
}
