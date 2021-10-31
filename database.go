package main

import (
	"errors"
	"godb/pager"
	"godb/table"
	"godb/table/types"
	"log"
	"math"
)

type Database struct {
	Pager           *pager.Pager
	TableDictionary *table.Table
}

var TABLE_DICTIONARY_SCHEMA = table.TableSchema{
	Columns: []table.ColumnDef{
		{Name: "Name", Type: types.TypeString},
		{Name: "FirstPageIdx", Type: types.TypeLong},
		{Name: "LastPageIdx", Type: types.TypeLong},
		{Name: "Schema", Type: types.TypeColDefs},
	},
}

func TableToDictionaryEntry(tbl *table.Table) table.Row {
	entry := make([]table.ColumnValue, len(TABLE_DICTIONARY_SCHEMA.Columns))
	entry[0] = types.String(tbl.Name)
	entry[1] = types.Long(tbl.FirstPageIdx)
	entry[2] = types.Long(tbl.LastPageIdx)
	entry[3] = types.ColDefs(tbl.Schema.Columns)
	return entry
}

// Write table information back to disk
func (db *Database) FlushTableDictionary(table *table.Table) error {
	entry := TableToDictionaryEntry(table)
	err := db.Update(db.TableDictionary, "Name", types.String(table.Name), entry)
	if err != nil {
		return err
	}

	// Update in-memory TableDictionary
	dict, err := db.OpenTable("TableDictionary")
	if err != nil {
		return err
	}

	db.TableDictionary = dict

	return nil
}

func (db *Database) OpenTable(tableName string) (*table.Table, error) {
	tableDictEntry, err := db.Select(db.TableDictionary, "Name", types.String(tableName))
	if err != nil {
		return nil, err
	}

	if !TABLE_DICTIONARY_SCHEMA.CheckSchema(tableDictEntry) {
		return nil, errors.New("OpenTable called with a row that doesn't belong to the TableDictionary")
	}
	var firstPageIdx int64
	var lastPageIdx int64
	var schema table.TableSchema

	for i, col := range TABLE_DICTIONARY_SCHEMA.Columns {
		switch col.Name {
		case "FirstPageIdx":
			firstPageIdx = int64(tableDictEntry[i].(types.Long))
		case "LastPageIdx":
			lastPageIdx = int64(tableDictEntry[i].(types.Long))
		case "Schema":
			schema = table.TableSchema{Columns: tableDictEntry[i].(types.ColDefs)}
		}
	}

	return &table.Table{
		Name:         tableName,
		Pager:        db.Pager,
		FirstPageIdx: firstPageIdx,
		LastPageIdx:  lastPageIdx,
		Schema:       schema,
	}, nil
}

func (db *Database) CreateTable(name string, schema table.TableSchema) (*table.Table, error) {
	row := table.Row{
		types.String(name),
		types.Long(-1),
		types.Long(-1),
		types.ColDefs(schema.Columns),
	}

	err := db.Insert(db.TableDictionary, row)
	if err != nil {
		return nil, err
	}

	tbl, err := db.OpenTable(name)
	if err != nil {
		return nil, err
	}

	return tbl, nil
}

func OpenDatabase(filename string) (*Database, error) {
	if pager.PAGE_SIZE > math.MaxInt16 {
		log.Fatal("Page size is greater than the range of int16. In page pointers would overflow.")
	}

	pager, err := pager.OpenPager(filename)
	if err != nil {
		return nil, err
	}

	tableDictionary := &table.Table{
		Name:         "TableDictionary",
		Pager:        pager,
		FirstPageIdx: -1,
		LastPageIdx:  -1,
		Schema:       TABLE_DICTIONARY_SCHEMA,
	}

	// Insert the TableDictionary table into the table dictionary.
	// This entry is probably not going to be used but it forces the pager to
	// actually allocate a page, thereby ensuring that the TableDictionary table
	// starts at page 0.
	row := table.Row{
		types.String("TableDictionary"),
		types.Long(0),
		types.Long(0),
		types.ColDefs(TABLE_DICTIONARY_SCHEMA.Columns),
	}

	db := &Database{
		Pager:           pager,
		TableDictionary: tableDictionary,
	}

	db.Insert(db.TableDictionary, row)

	return db, nil
}

func (db *Database) Close() {
	db.Pager.Close()
}
