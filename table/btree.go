package table

import (
	"encoding/binary"
	"fmt"
	"sort"

	"vqlite/pager"
)

const (
	maxCells = 12

	metaPageNum = uint32(0) // page 0 reserved for tree metadata
	metaRootOff = 0         // little-endian uint32 root page number
)

// BTree manages the overall tree: root page and table meta.
type BTree struct {
	rootPage  uint32     // page number of the root node
	bTreeMeta *BTreeMeta // convenience pointer for leaf/interior creation
}

// Cursor enables ordered traversal of the B+Tree.
type Cursor struct {
	tree  *BTree
	leaf  *LeafNode
	page  uint32
	idx   int
	valid bool
}

type BTreeMeta struct {
	Pager     *pager.Pager // for allocating pages, pageSize, etc.
	TableMeta *TableMeta   // schema, row sizes, max cells
}

// NewBTree opens or initializes a B+Tree.
// If the underlying pager has no pages yet, it allocates a new root leaf page
// and serializes an empty leaf node marked as root.
func NewBTree(p *pager.Pager, tblMeta *TableMeta) (*BTree, error) {
	btMeta := &BTreeMeta{Pager: p, TableMeta: tblMeta}

	// Case 1: brand-new file – allocate meta page (0) and root leaf (1).
	if p.NumPages == 0 {
		// Ensure meta page 0 exists
		if _, err := p.AllocatePage(); err != nil { // page 0
			return nil, err
		}

		// Create root leaf
		leaf, err := NewLeafNode(btMeta, true)
		if err != nil {
			return nil, fmt.Errorf("NewBTree: %w", err)
		}
		lp, _ := p.GetPage(leaf.Page())
		if err := leaf.Serialize(lp); err != nil {
			return nil, err
		}

		// Write root page number into meta page
		mp, _ := p.GetPage(metaPageNum)
		binary.LittleEndian.PutUint32(mp.Data[metaRootOff:metaRootOff+4], leaf.Page())
		mp.Dirty = true

		return &BTree{rootPage: leaf.Page(), bTreeMeta: btMeta}, nil
	}

	// Case 2: existing file – read root page number from meta page 0
	mp, err := p.GetPage(metaPageNum)
	if err != nil {
		return nil, err
	}
	rootPg := binary.LittleEndian.Uint32(mp.Data[metaRootOff : metaRootOff+4])
	return &BTree{rootPage: rootPg, bTreeMeta: btMeta}, nil
}

// Search looks for key in the tree.
// Returns the full row, true if found, or (nil,false,nil) if not.
func (t *BTree) Search(key uint32) (Row, bool, error) {
	// Use cursor's efficient Seek functionality
	cursor, err := t.NewCursor()
	if err != nil {
		return nil, false, err
	}

	// Seek to the target key
	if err := cursor.Seek(key); err != nil {
		return nil, false, err
	}

	// Check if we found the exact key
	if cursor.Valid() && cursor.Key() == key {
		return cursor.Value(), true, nil
	}

	return nil, false, nil
}

// Insert adds key+row into the tree, splitting and promoting at the root if needed.
func (t *BTree) Insert(key uint32, row Row) error {
	root, err := t.loadNode(t.rootPage)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	sibling, splitKey, didSplit := root.Insert(key, row)
	if !didSplit {
		return t.handleNoSplit(root)
	}

	return t.handleRootSplit(root, sibling, splitKey)
}

// Delete removes the given key from the tree.
// Returns true if the key was found and deleted, false if not found.
func (t *BTree) Delete(key uint32) (bool, error) {
	root, err := t.loadNode(t.rootPage)
	if err != nil {
		return false, fmt.Errorf("failed to load root node: %w", err)
	}

	found, _ := root.Delete(key)
	if !found {
		return false, nil // Key not found
	}

	// Serialize the root back to disk
	page, err := t.bTreeMeta.Pager.GetPage(t.rootPage)
	if err != nil {
		return false, fmt.Errorf("failed to get root page for serialization: %w", err)
	}

	if err := root.Serialize(page); err != nil {
		return false, fmt.Errorf("failed to serialize root node: %w", err)
	}

	return true, nil
}

// handleNoSplit handles the case where insertion doesn't cause a split.
func (t *BTree) handleNoSplit(root BTreeNode) error {
	page, err := t.bTreeMeta.Pager.GetPage(t.rootPage)
	if err != nil {
		return fmt.Errorf("failed to get root page for serialization: %w", err)
	}

	if err := root.Serialize(page); err != nil {
		return fmt.Errorf("failed to serialize root node: %w", err)
	}

	return nil
}

// handleRootSplit handles the case where the root splits and a new root needs to be created.
func (t *BTree) handleRootSplit(oldRoot, sibling BTreeNode, splitKey uint32) error {
	// Allocate new root page
	newRootPage, err := t.AllocatePage()
	if err != nil {
		return fmt.Errorf("failed to allocate new root page: %w", err)
	}

	// Update old root to no longer be root and serialize it
	if err := t.demoteOldRoot(oldRoot); err != nil {
		return fmt.Errorf("failed to demote old root: %w", err)
	}

	// Serialize the new sibling
	if err := t.serializeSibling(sibling); err != nil {
		return fmt.Errorf("failed to serialize sibling: %w", err)
	}

	// Create and serialize new interior root
	if err := t.createNewRoot(newRootPage, oldRoot, sibling, splitKey); err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}

	// Update tree's root pointer in memory and on disk
	if err := t.updateRootPointer(newRootPage); err != nil {
		return fmt.Errorf("failed to update root pointer: %w", err)
	}

	return nil
}

// demoteOldRoot clears the isRoot flag of the old root and re-serializes it.
func (t *BTree) demoteOldRoot(oldRoot BTreeNode) error {
	if hdr := rootHeader(oldRoot); hdr != nil {
		hdr.isRoot = false
		page, err := t.bTreeMeta.Pager.GetPage(oldRoot.Page())
		if err != nil {
			return fmt.Errorf("failed to get old root page: %w", err)
		}
		if err := oldRoot.Serialize(page); err != nil {
			return fmt.Errorf("failed to serialize demoted root: %w", err)
		}
	}
	return nil
}

// serializeSibling serializes the sibling node to its page.
func (t *BTree) serializeSibling(sibling BTreeNode) error {
	sibPage, err := t.bTreeMeta.Pager.GetPage(sibling.Page())
	if err != nil {
		return fmt.Errorf("failed to get sibling page: %w", err)
	}
	if err := sibling.Serialize(sibPage); err != nil {
		return fmt.Errorf("failed to serialize sibling: %w", err)
	}
	return nil
}

// createNewRoot builds and serializes the new interior root node.
func (t *BTree) createNewRoot(newRootPage uint32, oldRoot, sibling BTreeNode, splitKey uint32) error {
	newRoot := &InteriorNode{
		header: baseHeader{
			pageNum:      newRootPage,
			isRoot:       true,
			parentPage:   0,
			numCells:     1,
			rightPointer: sibling.Page(),
		},
		cells: []InteriorCell{
			{ChildPage: oldRoot.Page(), Key: splitKey},
		},
	}

	newRootPageObj, err := t.bTreeMeta.Pager.GetPage(newRootPage)
	if err != nil {
		return fmt.Errorf("failed to get new root page: %w", err)
	}

	if err := newRoot.Serialize(newRootPageObj); err != nil {
		return fmt.Errorf("failed to serialize new root: %w", err)
	}

	return nil
}

// updateRootPointer updates the tree's root pointer both in memory and on disk.
func (t *BTree) updateRootPointer(newRootPage uint32) error {
	t.rootPage = newRootPage

	metaPage, err := t.bTreeMeta.Pager.GetPage(metaPageNum)
	if err != nil {
		return fmt.Errorf("failed to get meta page: %w", err)
	}

	binary.LittleEndian.PutUint32(metaPage.Data[metaRootOff:metaRootOff+4], newRootPage)
	metaPage.Dirty = true

	return nil
}

// loadNode reads pageNum, inspects the first byte, and returns
// either a LeafNode (with meta) or InteriorNode.
func (t *BTree) loadNode(pageNum uint32) (BTreeNode, error) {
	p, err := t.bTreeMeta.Pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}

	switch p.Data[0] {
	case nodeTypeLeaf:
		return t.loadLeafNode(pageNum)

	case nodeTypeInterior:
		inode := &InteriorNode{bTreeMeta: t.bTreeMeta}
		inode.header.pageNum = pageNum
		if err := inode.Load(p); err != nil {
			return nil, err
		}
		inode.header.pageNum = pageNum
		return inode, nil

	default:
		return nil, fmt.Errorf("loadNode: unknown node type %d", p.Data[0])
	}
}

// AllocatePage hands out the next free page number.
func (t *BTree) AllocatePage() (uint32, error) {
	return t.bTreeMeta.Pager.AllocatePage()
}

// loadLeafNode creates a LeafNode bound to the given page and loads its data.
func (t *BTree) loadLeafNode(pageNum uint32) (*LeafNode, error) {
	p, err := t.bTreeMeta.Pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	leaf := &LeafNode{bTreeMeta: t.bTreeMeta}
	leaf.header.pageNum = pageNum
	if err := leaf.Load(p); err != nil {
		return nil, err
	}
	return leaf, nil
}

// rootHeader pulls the baseHeader out of a node, if possible.
func rootHeader(n BTreeNode) *baseHeader {
	switch v := n.(type) {
	case *LeafNode:
		return &v.header
	case *InteriorNode:
		return &v.header
	default:
		return nil
	}
}

// firstLeaf descends to the left–most leaf of the tree.
func (t *BTree) firstLeaf() (*LeafNode, uint32, error) {
	pgno := t.rootPage
	for {
		node, err := t.loadNode(pgno)
		if err != nil {
			return nil, 0, err
		}
		if node.IsLeaf() {
			return node.(*LeafNode), pgno, nil
		}
		in := node.(*InteriorNode)
		if len(in.cells) > 0 {
			pgno = in.cells[0].ChildPage
		} else {
			pgno = in.header.rightPointer
		}
	}
}

// NewCursor returns a cursor positioned at the first row (if any).
func (t *BTree) NewCursor() (*Cursor, error) {
	leaf, pg, err := t.firstLeaf()
	if err != nil {
		return nil, err
	}
	c := &Cursor{tree: t, leaf: leaf, page: pg}
	if leaf.header.numCells == 0 {
		c.valid = false
	} else {
		c.idx = 0
		c.valid = true
	}
	return c, nil
}

// Valid tells whether the cursor is positioned at an existing key/value.
func (c *Cursor) Valid() bool { return c.valid }

// Key returns the current key. Call only if Valid() is true.
func (c *Cursor) Key() uint32 { return c.leaf.cells[c.idx].Key }

// Value returns the current row. Call only if Valid() is true.
func (c *Cursor) Value() Row { return c.leaf.cells[c.idx].Value }

// Next advances to the next key in order.
func (c *Cursor) Next() error {
	if !c.valid {
		return nil
	}
	c.idx++
	if c.idx < int(c.leaf.header.numCells) {
		return nil
	}
	// move to next leaf via rightPointer
	if c.leaf.header.rightPointer == 0 {
		c.valid = false
		return nil
	}
	newLeaf, err := c.tree.loadLeafNode(c.leaf.header.rightPointer)
	if err != nil {
		return err
	}
	c.leaf = newLeaf
	c.page = newLeaf.Page()
	if newLeaf.header.numCells == 0 {
		c.valid = false
	} else {
		c.idx = 0
		c.valid = true
	}
	return nil
}

// findLeafForKey traverses the tree to find the leaf node that should contain the given key.
// Returns the leaf node and its page number.
func (t *BTree) findLeafForKey(key uint32) (*LeafNode, uint32, error) {
	pgno := t.rootPage
	for {
		node, err := t.loadNode(pgno)
		if err != nil {
			return nil, 0, err
		}
		if node.IsLeaf() {
			return node.(*LeafNode), pgno, nil
		}

		interior := node.(*InteriorNode)
		pgno = t.findChildPageInInterior(interior, key)
	}
}

// findChildPageInInterior finds the appropriate child page for a given key in an interior node.
// Uses binary search for efficiency, consistent with the Seek implementation.
func (t *BTree) findChildPageInInterior(interior *InteriorNode, key uint32) uint32 {
	// Binary search for the first cell with Key >= key
	idx := sort.Search(len(interior.cells), func(i int) bool {
		return interior.cells[i].Key >= key
	})

	if idx < len(interior.cells) {
		return interior.cells[idx].ChildPage
	}
	return interior.header.rightPointer
}

// Seek repositions the cursor to the first key >= target key.
func (c *Cursor) Seek(target uint32) error {
	// Find the appropriate leaf node
	leaf, pgno, err := c.tree.findLeafForKey(target)
	if err != nil {
		return err
	}

	// Binary search within the leaf for the target key
	idx := sort.Search(int(leaf.header.numCells), func(i int) bool {
		return leaf.cells[i].Key >= target
	})

	// Update cursor state
	c.leaf = leaf
	c.page = pgno
	c.idx = idx
	c.valid = idx < int(leaf.header.numCells)

	return nil
}

// KeyRowPair represents a key-value pair for bulk loading
type KeyRowPair struct {
	Key uint32
	Row Row
}

// BulkLoad efficiently loads a large number of sorted key-value pairs into the B+ tree.
// This method replaces the existing tree content and builds a new tree from scratch
// using a bottom-up approach for optimal performance.
//
// Requirements:
// - data must be sorted by key in ascending order
// - keys must be unique
// - data should contain at least one entry
//
// The algorithm works by:
// 1. Building leaf pages from left to right, packing them efficiently
// 2. Constructing interior levels bottom-up until reaching a single root
// 3. Replacing the old tree with the new efficiently constructed tree
func (t *BTree) BulkLoad(data []KeyRowPair) error {
	if len(data) == 0 {
		return fmt.Errorf("BulkLoad: empty data slice")
	}

	// Validate that data is sorted and has unique keys
	if err := t.validateBulkData(data); err != nil {
		return fmt.Errorf("BulkLoad: %w", err)
	}

	// Build the tree from bottom up
	newRootPage, err := t.buildTreeBottomUp(data)
	if err != nil {
		return fmt.Errorf("BulkLoad: failed to build tree: %w", err)
	}

	// Replace the old tree with the new one
	if err := t.replaceTree(newRootPage); err != nil {
		return fmt.Errorf("BulkLoad: failed to replace tree: %w", err)
	}

	return nil
}

// validateBulkData ensures the input data is properly sorted and contains unique keys
func (t *BTree) validateBulkData(data []KeyRowPair) error {
	if len(data) == 0 {
		return fmt.Errorf("data slice is empty")
	}

	// Check that keys are sorted and unique
	for i := 1; i < len(data); i++ {
		if data[i-1].Key >= data[i].Key {
			return fmt.Errorf("data is not sorted or contains duplicate keys at index %d (key %d >= %d)",
				i-1, data[i-1].Key, data[i].Key)
		}
	}

	// Validate that each row matches the table schema
	for i, pair := range data {
		if len(pair.Row) != t.bTreeMeta.TableMeta.NumCols {
			return fmt.Errorf("row at index %d has %d columns, expected %d",
				i, len(pair.Row), t.bTreeMeta.TableMeta.NumCols)
		}
	}

	return nil
}

// buildTreeBottomUp constructs the B+ tree using a bottom-up approach
func (t *BTree) buildTreeBottomUp(data []KeyRowPair) (uint32, error) {
	// Step 1: Build all leaf pages and collect their metadata
	leafPages, err := t.buildLeafLevel(data)
	if err != nil {
		return 0, fmt.Errorf("failed to build leaf level: %w", err)
	}

	// Step 2: If we only have one leaf, it becomes the root
	if len(leafPages) == 1 {
		// Mark the single leaf as root
		leaf, err := t.loadLeafNode(leafPages[0].PageNum)
		if err != nil {
			return 0, fmt.Errorf("failed to load single leaf: %w", err)
		}
		leaf.header.isRoot = true

		page, err := t.bTreeMeta.Pager.GetPage(leafPages[0].PageNum)
		if err != nil {
			return 0, fmt.Errorf("failed to get page for single leaf: %w", err)
		}

		if err := leaf.Serialize(page); err != nil {
			return 0, fmt.Errorf("failed to serialize single leaf root: %w", err)
		}

		return leafPages[0].PageNum, nil
	}

	// Step 3: Build interior levels bottom-up
	currentLevel := leafPages
	for len(currentLevel) > 1 {
		nextLevel, err := t.buildInteriorLevel(currentLevel)
		if err != nil {
			return 0, fmt.Errorf("failed to build interior level: %w", err)
		}
		currentLevel = nextLevel
	}

	// Step 4: Mark the final root and return its page number
	rootPageNum := currentLevel[0].PageNum
	root, err := t.loadNode(rootPageNum)
	if err != nil {
		return 0, fmt.Errorf("failed to load final root: %w", err)
	}

	if hdr := rootHeader(root); hdr != nil {
		hdr.isRoot = true
		page, err := t.bTreeMeta.Pager.GetPage(rootPageNum)
		if err != nil {
			return 0, fmt.Errorf("failed to get root page: %w", err)
		}
		if err := root.Serialize(page); err != nil {
			return 0, fmt.Errorf("failed to serialize root: %w", err)
		}
	}

	return rootPageNum, nil
}

// PageInfo represents metadata about a constructed page
type PageInfo struct {
	PageNum uint32
	MinKey  uint32 // smallest key in this subtree
}

// buildLeafLevel constructs all leaf pages and links them together
func (t *BTree) buildLeafLevel(data []KeyRowPair) ([]PageInfo, error) {
	var leafPages []PageInfo
	dataIdx := 0

	for dataIdx < len(data) {
		// Create a new leaf page
		leaf, err := NewLeafNode(t.bTreeMeta, false) // not root initially
		if err != nil {
			return nil, fmt.Errorf("failed to create leaf node: %w", err)
		}

		// Fill the leaf with data up to maxCells
		startIdx := dataIdx
		for dataIdx < len(data) && len(leaf.cells) < maxCells {
			pair := data[dataIdx]
			leaf.cells = append(leaf.cells, LeafCell{
				Key:   pair.Key,
				Value: pair.Row,
			})
			dataIdx++
		}

		leaf.header.numCells = uint32(len(leaf.cells))

		// Link to the next leaf if this isn't the last one
		if dataIdx < len(data) {
			// We'll set the rightPointer after creating the next leaf
			// For now, we'll update it in a second pass
		}

		// Serialize the leaf to disk
		page, err := t.bTreeMeta.Pager.GetPage(leaf.Page())
		if err != nil {
			return nil, fmt.Errorf("failed to get leaf page: %w", err)
		}
		if err := leaf.Serialize(page); err != nil {
			return nil, fmt.Errorf("failed to serialize leaf: %w", err)
		}

		// Record this leaf's info
		leafPages = append(leafPages, PageInfo{
			PageNum: leaf.Page(),
			MinKey:  data[startIdx].Key,
		})
	}

	// Second pass: set up rightPointer links between leaves
	for i := 0; i < len(leafPages)-1; i++ {
		leaf, err := t.loadLeafNode(leafPages[i].PageNum)
		if err != nil {
			return nil, fmt.Errorf("failed to load leaf for linking: %w", err)
		}

		leaf.header.rightPointer = leafPages[i+1].PageNum

		page, err := t.bTreeMeta.Pager.GetPage(leafPages[i].PageNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get leaf page for linking: %w", err)
		}
		if err := leaf.Serialize(page); err != nil {
			return nil, fmt.Errorf("failed to serialize linked leaf: %w", err)
		}
	}

	return leafPages, nil
}

// buildInteriorLevel constructs interior nodes from the pages at the level below
func (t *BTree) buildInteriorLevel(childPages []PageInfo) ([]PageInfo, error) {
	var interiorPages []PageInfo
	childIdx := 0

	for childIdx < len(childPages) {
		// Create a new interior node
		interior, err := NewInteriorNode(t.bTreeMeta, false) // not root initially
		if err != nil {
			return nil, fmt.Errorf("failed to create interior node: %w", err)
		}

		// Fill the interior with child pointers up to maxCells
		startIdx := childIdx

		// First child becomes the rightmost pointer (standard B+ tree structure)
		if childIdx < len(childPages) {
			interior.header.rightPointer = childPages[childIdx].PageNum
			childIdx++
		}

		// Add remaining children as regular cells
		for childIdx < len(childPages) && len(interior.cells) < maxCells {
			child := childPages[childIdx]
			interior.cells = append(interior.cells, InteriorCell{
				ChildPage: child.PageNum,
				Key:       child.MinKey,
			})
			childIdx++
		}

		interior.header.numCells = uint32(len(interior.cells))

		// Serialize the interior node
		page, err := t.bTreeMeta.Pager.GetPage(interior.Page())
		if err != nil {
			return nil, fmt.Errorf("failed to get interior page: %w", err)
		}
		if err := interior.Serialize(page); err != nil {
			return nil, fmt.Errorf("failed to serialize interior: %w", err)
		}

		// Record this interior node's info
		interiorPages = append(interiorPages, PageInfo{
			PageNum: interior.Page(),
			MinKey:  childPages[startIdx].MinKey, // minimum key in subtree
		})
	}

	return interiorPages, nil
}

// replaceTree updates the tree to use the new root and updates metadata
func (t *BTree) replaceTree(newRootPage uint32) error {
	// Update the tree's root page number
	t.rootPage = newRootPage

	// Update the metadata page with the new root
	metaPage, err := t.bTreeMeta.Pager.GetPage(metaPageNum)
	if err != nil {
		return fmt.Errorf("failed to get meta page: %w", err)
	}

	binary.LittleEndian.PutUint32(metaPage.Data[metaRootOff:metaRootOff+4], newRootPage)
	metaPage.Dirty = true

	return nil
}
