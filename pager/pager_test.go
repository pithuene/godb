package pager_test

import (
	"errors"
	"godb/pager"
	"os"
	"testing"
)

const TEST_FILE = "test.db"

func TestPager(t *testing.T) {
	os.Remove(TEST_FILE)
	pager, err := pager.OpenPager(TEST_FILE)
	if err != nil {
		t.Error(err)
	}

	_, err = pager.FetchPage(0)
	if err == nil {
		t.Error(errors.New("No error when fetching out of range"))
	}

	page, err := pager.AppendPage()
	if err != nil {
		t.Error(err)
	}

	fetchedPage, err := pager.FetchPage(page.Index)
	if err != nil {
		t.Error(err)
	}

	if page != fetchedPage {
		t.Error(errors.New("Fetch didn't return correct page"))
	}

	if page.Memory[0] != 0 {
		t.Error(errors.New("New Page not empty"))
	}

	page.Memory[0] = 255

	if fetchedPage.Memory[0] != 255 {
		t.Error(errors.New("Cached page not properly shared"))
	}

	page.Flush()

	buf, err := os.ReadFile(TEST_FILE)
	if err != nil {
		t.Error(err)
	}

	if buf[0] != 255 {
		t.Error(errors.New("Data not written to disk correctly"))
	}

	pager.Close()

	os.Remove(TEST_FILE)
}
