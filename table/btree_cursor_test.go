package table

import (
	"os"
	"reflect"
	"testing"
	"vqlite/column"
	"vqlite/pager"
)

// TestCursorIterate verifies in-order iteration across leaf boundaries.
func TestCursorIterate(t *testing.T) {
	tpFile, _ := os.CreateTemp("", "btcursor-*.db")
	tpFile.Close()
	defer os.Remove(tpFile.Name())

	pg, _ := pager.OpenPager(tpFile.Name())

	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	meta, _ := BuildTableMeta(schema)
	bt, err := NewBTree(pg, meta)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}

	keys := []uint32{50, 10, 70, 30, 60, 20, 40}
	for _, k := range keys {
		if err := bt.Insert(k, Row{k}); err != nil {
			t.Fatalf("insert %d: %v", k, err)
		}
	}

	// Expected order after sorting
	exp := []uint32{10, 20, 30, 40, 50, 60, 70}
	cur, err := bt.NewCursor()
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	var got []uint32
	for cur.Valid() {
		got = append(got, cur.Key())
		cur.Next()
	}
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("iteration order = %v; want %v", got, exp)
	}
}

// TestCursorSeek verifies Seek positions the cursor on the first key >= target.
func TestCursorSeek(t *testing.T) {
	pg, _ := pager.OpenPager(":memory:")
	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	meta, _ := BuildTableMeta(schema)
	bt, _ := NewBTree(pg, meta)

	for i := uint32(0); i < 10; i++ {
		bt.Insert(i*10, Row{i * 10})
	}

	cur, _ := bt.NewCursor()
	if err := cur.Seek(35); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if !cur.Valid() || cur.Key() != 40 {
		t.Fatalf("seek expected key 40, got %d valid=%v", cur.Key(), cur.Valid())
	}
}

// TestCursorSeekRangeQueries demonstrates using Seek for range queries and iterations.
func TestCursorSeekRangeQueries(t *testing.T) {
	pg, _ := pager.OpenPager(":memory:")
	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	meta, _ := BuildTableMeta(schema)
	bt, _ := NewBTree(pg, meta)

	// Insert test data: 10, 20, 30, 40, 50, 60, 70, 80, 90
	for i := uint32(1); i <= 9; i++ {
		key := i * 10
		bt.Insert(key, Row{key})
	}

	cursor, _ := bt.NewCursor()

	// Test 1: Seek to exact key that exists
	if err := cursor.Seek(50); err != nil {
		t.Fatalf("seek 50: %v", err)
	}
	if !cursor.Valid() || cursor.Key() != 50 {
		t.Errorf("seek 50: expected key 50, got %d valid=%v", cursor.Key(), cursor.Valid())
	}

	// Test 2: Seek to key that doesn't exist - should position at next higher key
	if err := cursor.Seek(35); err != nil {
		t.Fatalf("seek 35: %v", err)
	}
	if !cursor.Valid() || cursor.Key() != 40 {
		t.Errorf("seek 35: expected key 40, got %d valid=%v", cursor.Key(), cursor.Valid())
	}

	// Test 3: Range query - find all keys >= 55 and <= 75
	if err := cursor.Seek(55); err != nil {
		t.Fatalf("seek 55: %v", err)
	}
	var rangeKeys []uint32
	for cursor.Valid() && cursor.Key() <= 75 {
		rangeKeys = append(rangeKeys, cursor.Key())
		cursor.Next()
	}
	expectedRange := []uint32{60, 70}
	if !reflect.DeepEqual(rangeKeys, expectedRange) {
		t.Errorf("range query [55,75]: got %v, want %v", rangeKeys, expectedRange)
	}

	// Test 4: Seek beyond all keys - should be invalid
	if err := cursor.Seek(100); err != nil {
		t.Fatalf("seek 100: %v", err)
	}
	if cursor.Valid() {
		t.Errorf("seek 100: expected invalid cursor, but got key %d", cursor.Key())
	}

	// Test 5: Seek to minimum - should position at first key
	if err := cursor.Seek(1); err != nil {
		t.Fatalf("seek 1: %v", err)
	}
	if !cursor.Valid() || cursor.Key() != 10 {
		t.Errorf("seek 1: expected key 10, got %d valid=%v", cursor.Key(), cursor.Valid())
	}
}
