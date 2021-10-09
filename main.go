package main

import (
	"godb/table"
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
				{Name: "key", Type: table.TypeLong},
				{Name: "value", Type: table.TypeLong},
			},
		},
	}

	if err != nil {
		log.Fatal(err)
	}

	insertValue := []table.ColumnValue{
		table.LongFrom(1),
		table.LongFrom(12345),
	}
	err = newTable.Insert(insertValue)
	if err != nil {
		log.Fatal(err)
	}

	_, err = newTable.Select("key", table.LongFrom(1))
	if err != nil {
		log.Fatal(err)
	}

	db.Close()
}
