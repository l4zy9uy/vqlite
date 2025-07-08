package table

import (
	"os"
	"reflect"
	"testing"

	"vqlite/column"
	"vqlite/pager"
)

// helper to create a temporary pager file
type tempPager struct {
	*pager.Pager
	filename string
}

func newTempPager(t *testing.T) *tempPager {
	f, err := os.CreateTemp("", "testpager-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	pg, err := pager.OpenPager(f.Name())
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	return &tempPager{Pager: pg, filename: f.Name()}
}

func (tp *tempPager) cleanup() {
	tp.Pager.File.Close()
	os.Remove(tp.filename)
}

// TestLeafNode_SerializeLoad tests that inserting into a LeafNode, serializing and loading
// preserves keys and row data correctly.
func TestLeafNode_SerializeLoad(t *testing.T) {
	// Define a simple schema: id INT, name TEXT(8)
	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "name", Type: column.ColumnTypeText, MaxLength: 8},
	}
	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("BuildTableMeta: %v", err)
	}

	tp := newTempPager(t)
	defer tp.cleanup()

	// Obtain page 0 from pager
	page, err := tp.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}

	// Create a LeafNode and insert some rows
	leaf := &LeafNode{tableMeta: meta}
	leaf.header.pageNum = 0
	// header.isRoot and parentPage default to false/0

	tests := []Row{
		{uint32(10), "Alice"},
		{uint32(5), "Bob"},
		{uint32(20), "Carol"},
	}
	// Insert in arbitrary order
	for _, r := range tests {
		leaf.Insert(r[0].(uint32), r)
	}

	// Serialize to page
	if err := leaf.Serialize(page); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Load into a new LeafNode
	loaded := &LeafNode{tableMeta: meta}
	loaded.header.pageNum = 0
	if err := loaded.Load(page); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify number of cells
	if loaded.header.numCells != leaf.header.numCells {
		t.Errorf("numCells = %d; want %d", loaded.header.numCells, leaf.header.numCells)
	}

	wantKeys := []uint32{5, 10, 20}
	var gotKeys []uint32
	for _, c := range loaded.cells {
		gotKeys = append(gotKeys, c.Key)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Errorf("Keys = %v; want %v", gotKeys, wantKeys)
	}

	// Verify row values preserved in sorted order
	wantRows := []Row{
		{uint32(5), "Bob"},
		{uint32(10), "Alice"},
		{uint32(20), "Carol"},
	}
	for i, c := range loaded.cells {
		if !reflect.DeepEqual(c.Value, wantRows[i]) {
			t.Errorf("Cell %d row = %v; want %v", i, c.Value, wantRows[i])
		}
	}
}

// TestLeafNode_LoadError tests that Load returns an error when page is not a leaf.
func TestLeafNode_LoadError(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	page, err := tp.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	// Set page type to interior
	page.Data[0] = nodeTypeInterior

	loaded := &LeafNode{tableMeta: &TableMeta{}}
	err = loaded.Load(page)
	if err == nil {
		t.Errorf("Load should have failed for non-leaf page")
	}
}

// TestInteriorNode_SerializeLoad tests that Serialize/Load roundtrip for InteriorNode
func TestInteriorNode_SerializeLoad(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	pageNum := uint32(1)
	page, err := tp.GetPage(pageNum)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}

	// Build an InteriorNode with two cells
	interior := &InteriorNode{}
	interior.header.pageNum = pageNum
	interior.header.isRoot = true
	interior.header.numCells = 2
	interior.header.rightPointer = 3
	interior.cells = []InteriorCell{
		{ChildPage: 10, Key: 100},
		{ChildPage: 20, Key: 200},
	}

	// Serialize
	if err := interior.Serialize(page); err != nil {
		t.Fatalf("Interior Serialize failed: %v", err)
	}

	// Load into new node
	loaded := &InteriorNode{}
	if err := loaded.Load(page); err != nil {
		t.Fatalf("Interior Load failed: %v", err)
	}

	if loaded.header.numCells != interior.header.numCells {
		t.Errorf("numCells = %d; want %d", loaded.header.numCells, interior.header.numCells)
	}
	if loaded.header.rightPointer != interior.header.rightPointer {
		t.Errorf("rightPointer = %d; want %d", loaded.header.rightPointer, interior.header.rightPointer)
	}

	// Check cells
	if !reflect.DeepEqual(loaded.cells, interior.cells) {
		t.Errorf("Cells = %v; want %v", loaded.cells, interior.cells)
	}
}

// TestLeafNode_Insert_NoSplit ensures Insert maintains sorted order
// and correct counts when number of cells <= maxCells.
func TestLeafNode_Insert_NoSplit(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	// Simple schema with one INT column
	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("BuildTableMeta: %v", err)
	}
	btm := &BTreeMeta{Pager: tp.Pager, TableMeta: meta}

	// Create leaf at page 7
	leaf := NewLeafNode(btm, 7, true)

	keys := []uint32{42, 7, 99, 7}
	for i, k := range keys {
		newNode, splitKey, split := leaf.Insert(k, Row{k})
		if newNode != nil || splitKey != 0 || split {
			t.Errorf("Insert(%d) = (%v,%d,%v); want (nil,0,false)", k, newNode, splitKey, split)
		}
		if leaf.Page() != 7 {
			t.Errorf("Page() = %d; want 7", leaf.Page())
		}
		wantCount := uint32(i + 1)
		if leaf.header.numCells != wantCount {
			t.Errorf("numCells = %d; want %d", leaf.header.numCells, wantCount)
		}
	}

	// After all inserts, keys should be sorted (duplicates allowed)
	want := []uint32{7, 7, 42, 99}
	var got []uint32
	for _, c := range leaf.cells {
		got = append(got, c.Key)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sorted keys = %v; want %v", got, want)
	}
}

// TestLeafNode_Insert_Split verifies that inserting one more than maxCells
// triggers a split, with correct splitKey, node sizes, and page numbers.
func TestLeafNode_Insert_Split(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	meta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("BuildTableMeta: %v", err)
	}
	btm := &BTreeMeta{Pager: tp.Pager, TableMeta: meta}

	// Allocate initial page for the leaf so it's tracked by pager
	rootPg, err := tp.Pager.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	leaf := NewLeafNode(btm, rootPg, true)

	// Insert maxCells items (no split)
	for i := uint32(0); i < maxCells; i++ {
		newNode, _, split := leaf.Insert(i, Row{i})
		if split || newNode != nil {
			t.Fatalf("unexpected split on insert #%d", i)
		}
	}

	// Insert one more to exceed maxCells and trigger split
	newNode, splitKey, split := leaf.Insert(maxCells, Row{maxCells})
	if !split {
		t.Fatalf("expected split on insert #%d", maxCells)
	}
	if newNode == nil {
		t.Fatal("expected non-nil sibling node after split")
	}

	// Calculate expected page numbers
	wantSibPg := rootPg + 1
	if newNode.Page() != wantSibPg {
		t.Errorf("sibling Page() = %d; want %d", newNode.Page(), wantSibPg)
	}
	if leaf.header.rightPointer != wantSibPg {
		t.Errorf("rightPointer = %d; want %d", leaf.header.rightPointer, wantSibPg)
	}

	// Determine expected node sizes
	mid := (maxCells + 1) / 2
	wantLeft := mid
	wantRight := (maxCells + 1) - mid
	if leaf.header.numCells != uint32(wantLeft) {
		t.Errorf("left numCells = %d; want %d", leaf.header.numCells, wantLeft)
	}
	rn := newNode.(*LeafNode).header.numCells
	if rn := rn; rn != uint32(wantRight) {
		t.Errorf("right numCells = %d; want %d", rn, wantRight)
	}

	// splitKey should match first key in sibling
	firstKey := newNode.(*LeafNode).cells[0].Key
	if splitKey != firstKey {
		t.Errorf("splitKey = %d; want %d", splitKey, firstKey)
	}
}
