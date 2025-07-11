package main

import (
	"fmt"
	"os"
	"strings"
	"vqlite/column"
	"vqlite/pager"
	"vqlite/table"
)

func doMetaCommand(input string) MetaCommandResult {
	if input == ".exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(input string, stmt *Statement) PrepareResult {
	if strings.HasPrefix(input, "insert") {
		stmt.Type = StatementInsert
		return PrepareSuccess
	}
	if input == "select" {
		stmt.Type = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func executeStatement(stmt *Statement) {
	switch stmt.Type {
	case StatementInsert:
		fmt.Println("This is where we would do an insert.")
	case StatementSelect:
		fmt.Println("This is where we would do a select.")
	}
}

func main() {
	// 1) Define your schema: id INT, username TEXT(32), email TEXT(64), age INT
	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "username", Type: column.ColumnTypeText, MaxLength: 32},
		{Name: "email", Type: column.ColumnTypeText, MaxLength: 64},
		{Name: "age", Type: column.ColumnTypeInt},
	}

	// Open pager & B-tree (will create new tree if file empty)
	pg, err := pager.OpenPager("test.db")
	if err != nil {
		fmt.Println("open pager:", err)
		return
	}
	meta, _ := table.BuildTableMeta(schema)
	bt, err := table.NewBTree(pg, meta)
	if err != nil {
		fmt.Println("NewBTree:", err)
		return
	}

	// Insert rows
	rows := []table.Row{
		{uint32(1), "alice", "alice@example.com", uint32(30)},
		{uint32(2), "bob", "bob@example.com", uint32(25)},
	}
	for _, r := range rows {
		if err := bt.Insert(r[0].(uint32), r); err != nil {
			fmt.Println("insert:", err)
			return
		}
	}

	// Fetch and print
	for _, r := range rows {
		got, found, _ := bt.Search(r[0].(uint32))
		if !found {
			fmt.Println("row not found", r[0])
			continue
		}
		fmt.Printf("Row key %d: %v\n", r[0].(uint32), got)
	}
}
