package main

import (
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

func OpenDatabase(filename string) (*Database, error) {
	if pager.PAGE_SIZE > math.MaxInt16 {
		log.Fatal("Page size is greater than the range of int16. In page pointers would overflow.")
	}

	pager, err := pager.OpenPager(filename)
	if err != nil {
		return nil, err
	}

	tableDictionary := &table.Table{
		Pager:        pager,
		FirstPageIdx: -1,
		LastPageIdx:  -1,
		Schema: table.TableSchema{
			Columns: []table.ColumnDef{
				{Name: "Name", Type: types.TypeString},
				{Name: "FirstPageIdx", Type: types.TypeLong},
				{Name: "LastPageIdx", Type: types.TypeLong},
			},
		},
	}

	// Insert the TableDictionary table into the table dictionary.
	// This entry is probably not going to be used but it forces the pager to
	// actually allocate a page, thereby ensuring that the TableDictionary table
	// starts at page 0.
	row := table.Row{
		types.String("TableDictionary"),
		types.Long(0),
		types.Long(0),
	}

	tableDictionary.Insert(row)

	return &Database{
		Pager:           pager,
		TableDictionary: tableDictionary,
	}, nil
}

func (db *Database) Close() {
	db.Pager.Close()
}
