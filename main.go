package main

import (
	"fmt"
	"log"
)

func main() {
	fmt.Println("Hello World")

	db, err := OpenDatabase("database.db")

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
