package table

import (
	"os"
	"reflect"
	"testing"

	"vqlite/column"
	"vqlite/pager"
)

// tempPager wraps a Pager backed by a temporary on-disk file so each test
// has an isolated database. The file is removed in cleanup().
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

// TestLeafNode_SerializeLoad inserts a few rows, serializes the leaf to disk,
// loads it back, and verifies both keys and row values are preserved.
func TestLeafNode_SerializeLoad(t *testing.T) {
	// id INT, name TEXT(8)
	schema := column.Schema{
		{Name: "id", Type: column.ColumnTypeInt},
		{Name: "name", Type: column.ColumnTypeText, MaxLength: 8},
	}
	tblMeta, err := BuildTableMeta(schema)
	if err != nil {
		t.Fatalf("BuildTableMeta: %v", err)
	}

	tp := newTempPager(t)
	defer tp.cleanup()

	btMeta := &BTreeMeta{Pager: tp.Pager, TableMeta: tblMeta}

	leaf, err := NewLeafNode(btMeta, true)
	if err != nil {
		t.Fatalf("NewLeafNode: %v", err)
	}

	// Insert three rows out of order
	rows := []Row{
		{uint32(10), "Alice"},
		{uint32(5), "Bob"},
		{uint32(20), "Carol"},
	}
	for _, r := range rows {
		if _, _, split := leaf.Insert(r[0].(uint32), r); split {
			t.Fatalf("unexpected split during setup")
		}
	}

	// Serialize to its on-disk page
	page, err := tp.GetPage(leaf.Page())
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	if err := leaf.Serialize(page); err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	// Load into a fresh LeafNode instance
	loaded := &LeafNode{bTreeMeta: btMeta}
	loaded.header.pageNum = leaf.Page()
	if err := loaded.Load(page); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.header.numCells != leaf.header.numCells {
		t.Errorf("numCells = %d; want %d", loaded.header.numCells, leaf.header.numCells)
	}

	// Expect keys in ascending order 5,10,20
	wantKeys := []uint32{5, 10, 20}
	gotKeys := make([]uint32, 0, loaded.header.numCells)
	for _, c := range loaded.cells {
		gotKeys = append(gotKeys, c.Key)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Errorf("keys = %v; want %v", gotKeys, wantKeys)
	}

	// Check row contents preserve order
	wantRows := []Row{
		{uint32(5), "Bob"},
		{uint32(10), "Alice"},
		{uint32(20), "Carol"},
	}
	for i, c := range loaded.cells {
		if !reflect.DeepEqual(c.Value, wantRows[i]) {
			t.Errorf("row %d = %v; want %v", i, c.Value, wantRows[i])
		}
	}
}

// TestLeafNode_LoadError verifies Load returns an error when the page’s type byte
// does not indicate a leaf node.
func TestLeafNode_LoadError(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	pgno, err := tp.Pager.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	page, _ := tp.GetPage(pgno)
	page.Data[0] = nodeTypeInterior // mark as interior

	leaf := &LeafNode{bTreeMeta: &BTreeMeta{TableMeta: &TableMeta{}}}
	if err := leaf.Load(page); err == nil {
		t.Errorf("Load should fail for non-leaf page")
	}
}

// TestInteriorNode_SerializeLoad creates an interior node, serializes it, then
// loads it back and ensures the header and cell array round-trip intact.
func TestInteriorNode_SerializeLoad(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	pgno, _ := tp.Pager.AllocatePage()
	page, _ := tp.GetPage(pgno)

	interior := &InteriorNode{
		bTreeMeta: &BTreeMeta{},
		header: baseHeader{
			pageNum:      pgno,
			isRoot:       true,
			numCells:     2,
			rightPointer: 3,
		},
		cells: []InteriorCell{{ChildPage: 10, Key: 100}, {ChildPage: 20, Key: 200}},
	}

	if err := interior.Serialize(page); err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	loaded := &InteriorNode{bTreeMeta: &BTreeMeta{}}
	loaded.header.pageNum = pgno
	if err := loaded.Load(page); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.header.numCells != interior.header.numCells {
		t.Errorf("numCells = %d; want %d", loaded.header.numCells, interior.header.numCells)
	}
	if loaded.header.rightPointer != interior.header.rightPointer {
		t.Errorf("rightPointer = %d; want %d", loaded.header.rightPointer, interior.header.rightPointer)
	}
	if !reflect.DeepEqual(loaded.cells, interior.cells) {
		t.Errorf("cells = %v; want %v", loaded.cells, interior.cells)
	}
}

// TestLeafNode_Insert_NoSplit ensures inserts maintain sorted key order and
// no split occurs while the number of cells ≤ maxCells.
func TestLeafNode_Insert_NoSplit(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	tblMeta, _ := BuildTableMeta(schema)
	btMeta := &BTreeMeta{Pager: tp.Pager, TableMeta: tblMeta}

	leaf, err := NewLeafNode(btMeta, true)
	if err != nil {
		t.Fatalf("NewLeafNode: %v", err)
	}
	originalPage := leaf.Page()

	keys := []uint32{42, 7, 99, 7}
	for i, k := range keys {
		newNode, splitKey, split := leaf.Insert(k, Row{k})
		if newNode != nil || splitKey != 0 || split {
			t.Errorf("Insert(%d) = (%v,%d,%v); want (nil,0,false)", k, newNode, splitKey, split)
		}
		if leaf.Page() != originalPage {
			t.Errorf("Page changed from %d to %d", originalPage, leaf.Page())
		}
		if want := uint32(i + 1); leaf.header.numCells != want {
			t.Errorf("numCells = %d; want %d", leaf.header.numCells, want)
		}
	}

	wantKeys := []uint32{7, 7, 42, 99}
	got := make([]uint32, 0, len(leaf.cells))
	for _, c := range leaf.cells {
		got = append(got, c.Key)
	}
	if !reflect.DeepEqual(got, wantKeys) {
		t.Errorf("sorted keys = %v; want %v", got, wantKeys)
	}
}

// TestLeafNode_Insert_Split inserts maxCells+1 rows to trigger a split and
// validates the resulting sibling node, splitKey, and cell distribution.
func TestLeafNode_Insert_Split(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	tblMeta, _ := BuildTableMeta(schema)
	btMeta := &BTreeMeta{Pager: tp.Pager, TableMeta: tblMeta}

	leaf, err := NewLeafNode(btMeta, true)
	if err != nil {
		t.Fatalf("NewLeafNode: %v", err)
	}

	// Fill to capacity
	for i := uint32(0); i < maxCells; i++ {
		if n, _, split := leaf.Insert(i, Row{i}); split || n != nil {
			t.Fatalf("unexpected split while inserting %d", i)
		}
	}

	// One more insert should split
	sibling, splitKey, split := leaf.Insert(maxCells, Row{maxCells})
	if !split || sibling == nil {
		t.Fatalf("expected split on insert %d", maxCells)
	}

	// The rightPointer of the left node should point to the sibling’s page.
	if leaf.header.rightPointer != sibling.Page() {
		t.Errorf("rightPointer = %d; want %d", leaf.header.rightPointer, sibling.Page())
	}

	// Verify left/right cell counts
	mid := (maxCells + 1) / 2 // as computed in implementation
	if want := uint32(mid); leaf.header.numCells != want {
		t.Errorf("left numCells = %d; want %d", leaf.header.numCells, want)
	}
	if want := uint32((maxCells + 1) - mid); sibling.(*LeafNode).header.numCells != want {
		t.Errorf("right numCells = %d; want %d", sibling.(*LeafNode).header.numCells, want)
	}

	// splitKey should equal first key in sibling
	if firstKey := sibling.(*LeafNode).cells[0].Key; splitKey != firstKey {
		t.Errorf("splitKey = %d; want %d", splitKey, firstKey)
	}
}
