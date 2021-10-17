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
				{Name: "value", Type: table.TypeString},
			},
		},
	}

	if err != nil {
		log.Fatal(err)
	}

	insertValue := []table.ColumnValue{
		table.LongFrom(1),
		table.StringFrom("Hello World"),
	}
	err = newTable.Insert(insertValue)
	if err != nil {
		log.Fatal(err)
	}

	insertValue = []table.ColumnValue{
		table.LongFrom(2),
		table.StringFrom("Variable sizes implemented"),
	}
	err = newTable.Insert(insertValue)
	if err != nil {
		log.Fatal(err)
	}

	row, err := newTable.Select("key", table.LongFrom(1))
	if err != nil {
		log.Fatal(err)
	} else {
		for i, col := range row {
			log.Println(newTable.Schema.Columns[i].Name + ": " + col.String())
		}
	}

	db.Close()
}
