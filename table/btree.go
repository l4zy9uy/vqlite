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

// Search descends from the root with the given cursor, returns comparison result and error.
func (t *BTree) Search(c *Cursor, key uint32) (int, error) {
	root, err := t.loadNode(t.rootPage)
	if err != nil {
		return 0, err
	}
	return root.Search(c, key)
}

// Insert adds key+row into the tree, splitting and promoting at the root if needed.
func (t *BTree) Insert(c *Cursor, key uint32, row Row) error {
	leaf := c.leaf

	// 1) If key exists at cursor, overwrite
	if c.Valid() && leaf.cells[c.idx].Key == key {
		leaf.cells[c.idx].Value = row
		pg, err := t.bTreeMeta.Pager.GetPage(leaf.Page())
		if err != nil {
			return fmt.Errorf("insert: get leaf page: %w", err)
		}
		return leaf.Serialize(pg)
	}

	// 2) Otherwise insert into leaf
	sibling, splitKey, didSplit := leaf.Insert(c, key, row)
	pg, err := t.bTreeMeta.Pager.GetPage(leaf.Page())
	if err != nil {
		return fmt.Errorf("insert: get leaf page: %w", err)
	}
	if !didSplit {
		return leaf.Serialize(pg)
	}

	// 3) Propagate splits up
	var leftNode BTreeNode = leaf
	var rightNode BTreeNode = sibling
	upKey := splitKey

	for {
		parentPg := leftNode.(*InteriorNode).header.parentPage
		// reached root: build new root
		if parentPg == 0 {
			return t.handleRootSplit(leftNode, rightNode, upKey)
		}

		parent, err := t.loadNode(parentPg)
		if err != nil {
			return fmt.Errorf("insert: load parent page %d: %w", parentPg, err)
		}

		// splice into interior; pass cursor for API consistency
		newSib, newKey, split := parent.(*InteriorNode).Insert(c, upKey, row)
		if !split {
			ppg, _ := t.bTreeMeta.Pager.GetPage(parent.Page())
			return parent.Serialize(ppg)
		}

		leftNode = parent
		rightNode = newSib
		upKey = newKey
	}
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

// PageInfo represents a page during bulk loading with its minimum key
type PageInfo struct {
	pageNum uint32
	minKey  uint32
}

// buildAllLeaves creates and fills all leaf pages
func (t *BTree) buildAllLeaves(data []KeyRowPair) ([]*LeafNode, error) {
	var leaves []*LeafNode
	dataIdx := 0

	for dataIdx < len(data) {
		// Create a new leaf
		leaf, err := NewLeafNode(t.bTreeMeta, false)
		if err != nil {
			return nil, fmt.Errorf("failed to create leaf: %w", err)
		}

		// Fill leaf to capacity or until we run out of data
		for dataIdx < len(data) && len(leaf.cells) < maxCells {
			pair := data[dataIdx]
			leaf.cells = append(leaf.cells, LeafCell{
				Key:   pair.Key,
				Value: pair.Row,
			})
			dataIdx++
		}

		leaf.header.numCells = uint32(len(leaf.cells))

		// Serialize the leaf
		if err := t.serializeNode(leaf); err != nil {
			return nil, fmt.Errorf("failed to serialize leaf: %w", err)
		}

		leaves = append(leaves, leaf)
	}

	// Link leaves together with rightPointers
	for i := 0; i < len(leaves)-1; i++ {
		leaves[i].header.rightPointer = leaves[i+1].Page()
		if err := t.serializeNode(leaves[i]); err != nil {
			return nil, fmt.Errorf("failed to serialize linked leaf: %w", err)
		}
	}

	return leaves, nil
}

// serializeNode serializes a node to its page
func (t *BTree) serializeNode(node BTreeNode) error {
	page, err := t.bTreeMeta.Pager.GetPage(node.Page())
	if err != nil {
		return fmt.Errorf("failed to get page %d: %w", node.Page(), err)
	}
	return node.Serialize(page)
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
