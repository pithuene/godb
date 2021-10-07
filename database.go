package main

import "godb/pager"

type Database struct {
	Pager *pager.Pager
}

func OpenDatabase(filename string) (*Database, error) {
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
