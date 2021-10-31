package main

import (
	"errors"
	"godb/table"
)

func (db *Database) Insert(tbl *table.Table, row table.Row) error {
	if !tbl.Schema.CheckSchema(row) {
		return errors.New("Insert failed. Row doesn't conform to table schema.")
	}

	rowLen := row.Length()

	page, err := tbl.FindFreePage(rowLen)
	if err != nil {
		return err
	}

	entryBuffer, err := page.FindFreeEntry(rowLen)
	if err != nil {
		return err
	}

	row.Encode(entryBuffer)

	err = page.Flush()
	if err != nil {
		return err
	}

	err = db.FlushTableDictionary(tbl)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Allow multiple rows to be returned
func (db *Database) Select(tbl *table.Table, column string, targetValue table.ColumnValue) (table.Row, error) {
	colDef, colIdx, err := tbl.Schema.FindColumnByName(column)
	if err != nil {
		return nil, err
	}

	if colDef.Type != targetValue.Type() {
		return nil, errors.New("Select failed. Value type doesn't match")
	}

	page, err := tbl.FetchDataPage(tbl.FirstPageIdx)
	for {
		if err != nil { // pageIdx out of range. Key not found.
			return nil, errors.New("Select failed. Key not found!")
		}

		for entryIdx := int16(0); entryIdx < page.Header.RowPointersLength; entryIdx++ {
			entryBuffer, err := page.GetEntry(entryIdx)
			if err != nil {
				continue
			}

			row, _, err := tbl.DecodeRow(entryBuffer, tbl.Schema)
			if err != nil {
				return nil, err
			}

			compVal, err := row[colIdx].Compare(targetValue)
			if err != nil {
				return nil, err
			}

			if compVal == 0 {
				// FOUND!
				return row, nil
			}
		}
		page, err = tbl.NextPage(page)
	}
}

func (db *Database) Update(tbl *table.Table, targetColumn string, targetValue table.ColumnValue, newRow table.Row) error {
	if !tbl.Schema.CheckSchema(newRow) {
		return errors.New("Updated row has different schema")
	}

	colDef, colIdx, err := tbl.Schema.FindColumnByName(targetColumn)
	if err != nil {
		return err
	}

	if colDef.Type != targetValue.Type() {
		return errors.New("Update failed. Target value type doesn't match")
	}

	page, err := tbl.FetchDataPage(tbl.FirstPageIdx)
	for {
		if err != nil { // pageIdx out of range. Key not found.
			return errors.New("Update failed. Key not found!")
		}

		for entryIdx := int16(0); entryIdx < page.Header.RowPointersLength; entryIdx++ {
			entryBuffer, err := page.GetEntry(entryIdx)
			if err != nil {
				continue
			}

			row, _, err := tbl.DecodeRow(entryBuffer, tbl.Schema)
			if err != nil {
				return err
			}

			compVal, err := row[colIdx].Compare(targetValue)
			if err != nil {
				return err
			}

			if compVal == 0 {
				// TODO: Handle variable size data types.
				newRow.Encode(entryBuffer)
				err = page.Flush()
				if err != nil {
					return err
				}
				return nil
			}
		}
		page, err = tbl.NextPage(page)
	}
}
