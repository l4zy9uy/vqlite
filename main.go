package main

import (
	"fmt"
	"os"
	"strings"
	"vqlite/column"
	table2 "vqlite/table"
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

	// 2) Open (or create) the database file with that schema
	table, err := table2.OpenTable("test.db", schema)
	if err != nil {
		fmt.Println("Error opening table:", err)
		return
	}
	// Ensure we flush & close on exit
	defer func() {
		if err := table.Close(); err != nil {
			fmt.Println("Error closing table:", err)
		}
	}()

	// 3) Insert a couple of rows
	toInsert := []table2.Row{
		{uint32(1), "alice", "alice@example.com", uint32(30)},
		{uint32(2), "bob", "bob@example.com", uint32(25)},
	}

	for _, r := range toInsert {
		if err := table.InsertRow(r); err != nil {
			fmt.Println("Error inserting row:", err)
			return
		}
	}
	fmt.Printf("Inserted %d rows.\n\n", table.NumRows)

	// 4) Read them back and print
	for i := uint32(0); i < table.NumRows; i++ {
		rowVals, err := table.GetRow(i)
		if err != nil {
			fmt.Println("Error fetching row:", err)
			return
		}
		fmt.Printf("Row %d:\n", i)
		for j, val := range rowVals {
			fmt.Printf("  %s = %v\n", schema[j].Name, val)
		}
		fmt.Println()
	}
}
