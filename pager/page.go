package pager

import (
	"os"

	"golang.org/x/sys/unix"
)

var PAGE_SIZE = int64(os.Getpagesize())

type Page struct {
	// The index of the page in the file
	Index int64
	// The memory memory mapped buffer
	Memory []byte
}

func (page *Page) Flush() error {
	return unix.Msync(page.Memory, unix.MS_SYNC)
}
