package table

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"
	"vqlite/column"
)

func newTempDB(t *testing.T) string {
	f, err := os.CreateTemp("", "testdb-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	return f.Name()
}

func TestBuildTableMeta(t *testing.T) {
	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "name", Type: column.ColumnTypeText, MaxLength: 16},
		{Name: "score", Type: column.ColumnTypeInt},
	}
	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("BuildTableMeta failed: %v", err)
	}

	if meta.NumCols != 3 {
		t.Errorf("NumCols = %d; want 3", meta.NumCols)
	}

	wantOffsets := []uint32{0, 4, 20}
	for i, cm := range meta.Columns {
		if cm.Offset != wantOffsets[i] {
			t.Errorf("Column %q offset = %d; want %d", cm.Name, cm.Offset, wantOffsets[i])
		}
	}

	if meta.RowSize != 24 {
		t.Errorf("TotalRowSize = %d; want 24", meta.RowSize)
	}
}

func TestSerializeDeserializeRow(t *testing.T) {
	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "text", Type: column.ColumnTypeText, MaxLength: 8},
	}
	meta, _ := BuildTableMeta(schema)

	orig := Row{uint32(0xdeadbeef), "hello"}
	buf := make([]byte, meta.RowSize)
	if err := SerializeRow(meta, orig, buf); err != nil {
		t.Fatalf("SerializeRow error: %v", err)
	}

	if got := binary.LittleEndian.Uint32(buf[:4]); got != 0xdeadbeef {
		t.Errorf("Invalid int bytes: got 0x%x", got)
	}

	if string(buf[4:12]) != "hello\x00\x00\x00" {
		t.Errorf("Invalid text bytes: %q", buf[4:12])
	}

	row2, err := DeserializeRow(meta, buf)
	if err != nil {
		t.Fatalf("DeserializeRow error: %v", err)
	}
	if !reflect.DeepEqual(orig, row2) {
		t.Errorf("Roundtrip mismatch: got %+v; want %+v", row2, orig)
	}
}

func TestInsertGetRow_FileBacked(t *testing.T) {
	dbFile := newTempDB(t)
	defer os.Remove(dbFile)

	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "name", Type: column.ColumnTypeText, MaxLength: 16},
	}
	table, err := OpenTable(dbFile, schema)
	if err != nil {
		t.Fatalf("OpenTable: %v", err)
	}
	defer table.Close()

	rows := []Row{
		{uint32(1), "Alice"},
		{uint32(2), "Bob"},
		{uint32(3), "Carol"},
	}
	for _, r := range rows {
		if err := table.InsertRow(r); err != nil {
			t.Fatalf("insertRow: %v", err)
		}
	}

	if table.NumRows != uint32(len(rows)) {
		t.Errorf("NumRows = %d; want %d", table.NumRows, len(rows))
	}

	for i, want := range rows {
		got, err := table.GetRow(uint32(i))
		if err != nil {
			t.Fatalf("getRow(%d): %v", i, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("Row %d = %+v; want %+v", i, got, want)
		}
	}
}
