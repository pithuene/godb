package main

import (
	"godb/table"
	"godb/table/types"
	"log"
	"os"
)

const DATABASE_FILE = "database.db"

func main() {
	os.Remove(DATABASE_FILE)
	db, err := OpenDatabase(DATABASE_FILE)

	// TODO: Persist tables
	newTable := &table.Table{
		Pager:        db.Pager,
		FirstPageIdx: -1,
		LastPageIdx:  -1,
		Schema: table.TableSchema{
			Columns: []table.ColumnDef{
				{Name: "key", Type: types.TypeLong},
				{Name: "value", Type: types.TypeString},
			},
		},
	}

	if err != nil {
		log.Fatal(err)
	}

	insertValue := []table.ColumnValue{
		types.Long(1),
		types.String("Hello World"),
	}
	err = newTable.Insert(insertValue)
	if err != nil {
		log.Fatal(err)
	}

	insertValue = []table.ColumnValue{
		types.Long(2),
		types.String("Variable sizes implemented"),
	}
	err = newTable.Insert(insertValue)
	if err != nil {
		log.Fatal(err)
	}

	row, err := newTable.Select("key", types.Long(1))
	if err != nil {
		log.Fatal(err)
	} else {
		for i, col := range row {
			log.Println(newTable.Schema.Columns[i].Name + ": " + col.String())
		}
	}

	db.Close()
}
