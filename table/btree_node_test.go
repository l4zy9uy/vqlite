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

// TestInteriorNode_Insert_LeafSplit ensures that when a child leaf overflows
// and splits, the parent interior absorbs a new cell without overflowing.
func TestInteriorNode_Insert_LeafSplit(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	// Simple INT schema for rows (only the key is stored)
	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	tblMeta, _ := BuildTableMeta(schema)
	btMeta := &BTreeMeta{Pager: tp.Pager, TableMeta: tblMeta}

	// Create a leaf node that will sit under the interior root
	leaf, err := NewLeafNode(btMeta, false)
	if err != nil {
		t.Fatalf("NewLeafNode: %v", err)
	}

	// Fill leaf to capacity (maxCells) without triggering split
	for i := uint32(0); i < maxCells; i++ {
		if _, _, split := leaf.Insert(i, Row{i}); split {
			t.Fatalf("unexpected split while seeding leaf (i=%d)", i)
		}
	}

	// Serialize leaf to disk so the interior can load it later
	leafPg, _ := tp.GetPage(leaf.Page())
	if err := leaf.Serialize(leafPg); err != nil {
		t.Fatalf("serialize leaf: %v", err)
	}

	// Build an interior root whose rightPointer points to the leaf
	root, err := NewInteriorNode(btMeta, true)
	if err != nil {
		t.Fatalf("NewInteriorNode: %v", err)
	}
	root.header.rightPointer = leaf.Page()

	// Insert a key that will cause the child leaf to split
	newKey := uint32(maxCells) // one greater than existing max key in leaf
	newRow := Row{newKey}
	newNode, splitKey, split := root.Insert(newKey, newRow)

	// The root itself should *not* split in this scenario
	if split || newNode != nil || splitKey != 0 {
		t.Fatalf("root.Insert returned unexpected split (node=%v, key=%d, split=%v)", newNode, splitKey, split)
	}

	// After the operation, root should have exactly one cell referencing the new sibling
	if root.header.numCells != 1 {
		t.Fatalf("root header.numCells = %d; want 1", root.header.numCells)
	}

	// The key promoted from the leaf split should be the first key of the sibling leaf
	promotedKey := root.cells[0].Key
	expectedPromoted := uint32(maxCells / 2)
	if promotedKey != expectedPromoted {
		t.Errorf("promoted key = %d; want %d", promotedKey, expectedPromoted)
	}

	// Ensure the child page numbers are valid and distinct
	if root.cells[0].ChildPage == leaf.Page() {
		t.Errorf("ChildPage for new cell should be sibling, got original leaf page %d", leaf.Page())
	}
}

// TestInteriorNode_Insert_InteriorSplit builds an interior node already at
// capacity (maxCells).  Inserting a key causes the rightmost leaf to split,
// which in turn overflows the interior. We expect the interior itself to split
// and propagate upward (Insert should return (sibling, splitKey, true)).
func TestInteriorNode_Insert_InteriorSplit(t *testing.T) {
	tp := newTempPager(t)
	defer tp.cleanup()

	schema := column.Schema{{Name: "id", Type: column.ColumnTypeInt}}
	tblMeta, _ := BuildTableMeta(schema)
	btMeta := &BTreeMeta{Pager: tp.Pager, TableMeta: tblMeta}

	// Helper to make a leaf with a single key value
	makeLeafWithKey := func(k uint32) *LeafNode {
		leaf, err := NewLeafNode(btMeta, false)
		if err != nil {
			t.Fatalf("NewLeafNode: %v", err)
		}
		leaf.Insert(k, Row{k})
		pg, _ := tp.GetPage(leaf.Page())
		if err := leaf.Serialize(pg); err != nil {
			t.Fatalf("serialize leaf %d: %v", k, err)
		}
		return leaf
	}

	// Create leaves for each cell plus a rightmost leaf that is *full* so it
	// will split upon one more insert.
	var leaves []*LeafNode
	keysForCells := make([]uint32, 0, maxCells)
	for i := 0; i < maxCells; i++ {
		k := uint32(i*10 + 5) // 5,15,25,...
		keysForCells = append(keysForCells, k)
		leaves = append(leaves, makeLeafWithKey(k))
	}

	// Rightmost leaf filled to capacity
	rightLeaf, err := NewLeafNode(btMeta, false)
	if err != nil {
		t.Fatalf("NewLeafNode right: %v", err)
	}
	for i := uint32(0); i < maxCells; i++ {
		if _, _, split := rightLeaf.Insert(1000+i, Row{1000 + i}); split {
			t.Fatalf("unexpected split while seeding right leaf")
		}
	}
	pgRight, _ := tp.GetPage(rightLeaf.Page())
	if err := rightLeaf.Serialize(pgRight); err != nil {
		t.Fatalf("serialize right leaf: %v", err)
	}

	// Build the interior root at capacity
	root, err := NewInteriorNode(btMeta, true)
	if err != nil {
		t.Fatalf("NewInteriorNode: %v", err)
	}
	for i, k := range keysForCells {
		root.cells = append(root.cells, InteriorCell{ChildPage: leaves[i].Page(), Key: k})
	}
	root.header.numCells = uint32(maxCells)
	root.header.rightPointer = rightLeaf.Page()

	// Insert a key that will land in the rightmost leaf, forcing it to split
	bigKey := uint32(5000)
	newNode, splitKey, split := root.Insert(bigKey, Row{bigKey})

	if !split || newNode == nil {
		t.Fatalf("expected root to split; got split=%v newNode=%v", split, newNode)
	}

	// Validate sibling is an interior node and has expected number of cells
	sibInt, ok := newNode.(*InteriorNode)
	if !ok {
		t.Fatalf("sibling is not *InteriorNode, got %T", newNode)
	}

	// After split: left and right should each have maxCells/2 cells.
	leftCells := int(root.header.numCells)
	rightCells := int(sibInt.header.numCells)
	mid := maxCells / 2
	if leftCells != mid {
		t.Errorf("left numCells = %d; want %d", leftCells, mid)
	}
	if rightCells != maxCells-mid {
		t.Errorf("right numCells = %d; want %d", rightCells, maxCells-mid)
	}

	// The splitKey should equal the promoted median key
	expectedMed := keysForCells[mid]
	if splitKey != expectedMed {
		t.Errorf("splitKey = %d; want %d", splitKey, expectedMed)
	}
}
