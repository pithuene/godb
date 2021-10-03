package main

import (
	"fmt"
	"log"
)

func main() {
	fmt.Println("Hello World")
	pager, err := OpenPager("database.db")
	if err != nil {
		log.Fatal(err)
	}

	page, err := pager.AppendRawPage()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < int(PAGE_SIZE); i++ {
		page.Memory[i] = 100
	}
	fmt.Println(page)

	err = page.Flush()
	if err != nil {
		log.Fatal(err)
	}

	pager.Close()
}
