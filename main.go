package main

import (
	"bufio"
	"errors"
	"fmt"
	"godb/table"
	"godb/table/types"
	"log"
	"os"
	"strings"

	"github.com/SananGuliyev/sqlparser"
)

const DATABASE_FILE = "database.db"

func main() {
	types.InitializeTypeIds()

	os.Remove(DATABASE_FILE)
	db, err := OpenDatabase(DATABASE_FILE)

	newTable, err := db.CreateTable("Test",
		table.TableSchema{
			Columns: []table.ColumnDef{
				{Name: "key", Type: types.TypeLong},
				{Name: "value", Type: types.TypeString},
			},
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	insertValue := []table.ColumnValue{
		types.Long(1),
		types.String("Hello World"),
	}
	err = db.Insert(newTable, insertValue)
	if err != nil {
		log.Fatal(err)
	}

	insertValue = []table.ColumnValue{
		types.Long(2),
		types.String("Variable sizes implemented"),
	}
	err = db.Insert(newTable, insertValue)
	if err != nil {
		log.Fatal(err)
	}

	// REPL
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		// "SELECT * FROM TableDictionary WHERE Name = 'TableDictionary'"
		sql, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		if strings.HasPrefix(sql, "exit") {
			break
		}

		err = db.ExecSQL(sql)
		if err != nil {
			fmt.Println(err)
		}
	}

	db.Close()
}

func (db *Database) ExecSQL(sql string) error {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		if len(stmt.From) != 1 {
			return errors.New("Only one source supported in FROM")
		}

		tableName := sqlparser.GetTableName(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr).String()

		tbl, err := db.OpenTable(tableName)
		if err != nil {
			return err
		}

		whereExpr := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if whereExpr.Operator != "=" {
			return errors.New("Only = operator is supported for now")
		}
		whereColumn := whereExpr.Left.(*sqlparser.ColName).Name.String()
		whereSqlValue := whereExpr.Right.(*sqlparser.SQLVal)
		if whereSqlValue.Type != sqlparser.StrVal {
			return errors.New("Only strings values supported in WHERE")
		}
		whereValue := string(whereSqlValue.Val)

		row, err := db.Select(tbl, whereColumn, types.String(whereValue))
		if err != nil {
			log.Fatal(err)
		} else {
			for i, col := range row {
				fmt.Println(tbl.Schema.Columns[i].Name + ": " + col.String())
			}
		}

	default:
		log.Println("ERROR: Unknown statement type")
	}
	return nil
}
