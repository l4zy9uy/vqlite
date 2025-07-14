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

	// Create cursor for lookups
	cursor, err := bt.NewCursor()
	if err != nil {
		fmt.Println("create cursor:", err)
		return
	}

	// Fetch and print using cursor.Seek()
	for _, r := range rows {
		key := r[0].(uint32)
		if err := cursor.Seek(key); err != nil {
			fmt.Printf("seek error for key %d: %v\n", key, err)
			continue
		}
		if !cursor.Valid() || cursor.Key() != key {
			fmt.Printf("Row key %d: not found\n", key)
			continue
		}
		fmt.Printf("Row key %d: %v\n", key, cursor.Value())
	}

	// Demonstrate the power of cursor seeking with more examples
	fmt.Println("\n--- Demonstrating Cursor Seeking Power ---")

	// Example 1: Exact key lookup
	fmt.Println("1. Exact key lookup for key 2:")
	if err := cursor.Seek(2); err != nil {
		fmt.Println("   Seek error:", err)
	} else if cursor.Valid() && cursor.Key() == 2 {
		fmt.Printf("   Found: %v\n", cursor.Value())
	} else {
		fmt.Println("   Key 2 not found")
	}

	// Example 2: Find first key >= target (range start)
	fmt.Println("2. Find first key >= 1.5 (should position at key 2):")
	if err := cursor.Seek(1); err != nil { // Note: seeking to 1, should find 2
		fmt.Println("   Seek error:", err)
	} else if cursor.Valid() {
		fmt.Printf("   First key >= 1: %d with value %v\n", cursor.Key(), cursor.Value())
	} else {
		fmt.Println("   No keys >= 1")
	}

	// Example 3: Range iteration - all keys >= 1
	fmt.Println("3. Range iteration: all users with id >= 1:")
	if err := cursor.Seek(1); err != nil {
		fmt.Println("   Seek error:", err)
	} else {
		count := 0
		for cursor.Valid() {
			fmt.Printf("   - User %d: %s <%s>\n", cursor.Key(), cursor.Value()[1], cursor.Value()[2])
			count++
			cursor.Next()
		}
		fmt.Printf("   Total: %d users\n", count)
	}

	// Example 4: Key not found - cursor positioning
	fmt.Println("4. Seek to non-existent key 10 (should be invalid):")
	if err := cursor.Seek(10); err != nil {
		fmt.Println("   Seek error:", err)
	} else if cursor.Valid() {
		fmt.Printf("   Unexpected: found key %d\n", cursor.Key())
	} else {
		fmt.Println("   Correctly positioned: cursor invalid (key 10 > all existing keys)")
	}
}
