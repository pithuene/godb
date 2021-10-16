package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/pager"
)

type Table struct {
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
			Next:      -1,
			Prev:      table.LastPageIdx,
			EntrySize: table.EntrySize(),
		},
	}
	dataPage.Header.FreeEntries = dataPage.EntryCapacity()
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

// The byte length of a row
func (table *Table) EntrySize() int64 {
	size := int64(binary.Size(EntryHeader{}))
	for _, column := range table.Schema.Columns {
		size += column.Type.Length
	}
	return size
}

// Find page which still has free entries and create one if there is none.
// TODO: Currently iterates through all pages. This should be handled using a free list or the like.
func (table *Table) FindFreePage() (*DataPage, error) {
	dpage, err := table.FetchDataPage(table.FirstPageIdx)

	for {
		if err != nil { // Error occurs when pageIdx is out of range, so there is no free space on any page
			dpage, err = table.NewDataPage()
			if err != nil {
				return nil, err
			}
			return dpage, nil
		}
		if dpage.Header.FreeEntries > 0 {
			return dpage, nil
		} else {
			dpage, err = table.NextPage(dpage)
		}
	}
}

func (table *Table) Insert(row Row) error {
	if !table.Schema.CheckSchema(row) {
		return errors.New("Insert failed. Row doesn't conform to table schema.")
	}

	page, err := table.FindFreePage()

	entryBuffer, err := page.FindFreeEntry()
	if err != nil {
		return err
	}

	offset := binary.Size(EntryHeader{})
	for _, column := range row {
		// TODO: There is probably a better way to insert a slice into another one
		encoded := column.Encode()
		for i := 0; i < int(column.Type().Length); i++ {
			entryBuffer[offset+i] = encoded[i]
		}
		offset += int(column.Type().Length)
	}

	page.Flush()

	return nil
}

func (table *Table) Select(column string, value ColumnValue) (Row, error) {
	colDef, colIdx, err := table.Schema.FindColumnByName(column)
	if err != nil {
		return nil, err
	}

	if colDef.Type != value.Type() {
		return nil, errors.New("Select failed. Value type doesn't match")
	}

	page, err := table.FetchDataPage(table.FirstPageIdx)
	for {
		if err != nil { // pageIdx out of range. Key not found.
			return nil, errors.New("Key not found")
		}

		for entryIdx := int64(0); entryIdx < page.EntryCapacity(); entryIdx++ {
			entryBuffer, err := page.GetEntry(entryIdx)
			if err != nil {
				return nil, err
			}

			entryHeader := DeserializeEntryHeader(entryBuffer[0])
			if entryHeader.InUse {
				targetColumnOffset := binary.Size(EntryHeader{})
				for i := 0; i < colIdx; i++ {
					targetColumnOffset += int(table.Schema.Columns[i].Type.Length)
				}
				targetColumnRaw := entryBuffer[targetColumnOffset : targetColumnOffset+int(colDef.Type.Length)]
				columnValue, err := colDef.Type.Decode(targetColumnRaw)
				if err != nil {
					return nil, err
				}

				compVal, err := columnValue.Compare(value)
				if err != nil {
					return nil, err
				}

				if compVal == 0 {
					// FOUND!
					row, err := table.decodeRow(entryBuffer, table.Schema)
					if err != nil {
						return nil, err
					}

					return row, nil
				}
			}
		}
		page, err = table.NextPage(page)
	}
}

func (table *Table) decodeRow(buffer []byte, schema TableSchema) (Row, error) {
	row := make([]ColumnValue, len(table.Schema.Columns))
	offset := int64(binary.Size(EntryHeader{}))
	for i, colDef := range table.Schema.Columns {
		val, err := colDef.Type.Decode(buffer[offset : offset+colDef.Type.Length])
		if err != nil {
			return nil, err
		}

		row[i] = val

		offset += colDef.Type.Length
	}
	return row, nil
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

	/*next, n := binary.Varint(page.Memory[0:8])
	if n <= 0 {
		return nil, errors.New("Next deserialization failed")
	}
	dataPage.Header.Next = next

	prev, n := binary.Varint(page.Memory[8:16])
	if n <= 0 {
		return nil, errors.New("Prev deserialization failed")
	}
	dataPage.Header.Prev = prev

	entrySize, n := binary.Varint(page.Memory[16:24])
	if n <= 0 {
		return nil, errors.New("EntrySize deserialization failed")
	}
	dataPage.Header.EntrySize = entrySize

	freeEntries, n := binary.Varint(page.Memory[24:32])
	if n <= 0 {
		return nil, errors.New("FreeEntries deserialization failed")
	}
	dataPage.Header.FreeEntries = freeEntries*/

	return dataPage, nil
}
