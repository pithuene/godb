package main

import (
	"fmt"
	"log"
	"os"
)

const DATABASE_FILE = "database.db"

func main() {
	fmt.Println("Hello World")

	os.Remove(DATABASE_FILE)
	db, err := OpenDatabase(DATABASE_FILE)

	// TODO: Persist tables
	table := &Table{
		database:     db,
		FirstPageIdx: -1,
		LastPageIdx:  -1,
	}

	if err != nil {
		log.Fatal(err)
	}

	err = table.Insert(1, 345)
	if err != nil {
		log.Fatal(err)
	}

	res, err := table.Select(1)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(res)

	db.Close()
}
