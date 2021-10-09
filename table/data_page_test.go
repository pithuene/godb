package table_test

import (
	"os"
	"testing"
)

const TEST_FILE = "test.db"

func TestTable(t *testing.T) {
	os.Remove(TEST_FILE)

	os.Remove(TEST_FILE)
}
