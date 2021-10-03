package main

import (
	"fmt"
	"log"
)

func main() {
	fmt.Println("Hello World")

	db, err := OpenDatabase("database.db")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Insert(1, 345)
	if err != nil {
		log.Fatal(err)
	}

	res, err := db.Select(1)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(res)

	db.Close()
}
