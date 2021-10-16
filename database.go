package main

import (
	"godb/pager"
	"log"
	"math"
)

type Database struct {
	Pager *pager.Pager
}

func OpenDatabase(filename string) (*Database, error) {
	if pager.PAGE_SIZE > math.MaxInt16 {
		log.Fatal("Page size is greater than the range of int16. In page pointers would overflow.")
	}

	pager, err := pager.OpenPager(filename)
	if err != nil {
		return nil, err
	}
	return &Database{
		Pager: pager,
	}, nil
}

func (db *Database) Close() {
	db.Pager.Close()
}
