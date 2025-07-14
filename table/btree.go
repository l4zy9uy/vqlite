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
	node, err := t.loadNode(t.rootPage)
	if err != nil {
		return nil, false, err
	}
	return t.searchNode(node, key)
}

// recursive helper for Search
func (t *BTree) searchNode(node BTreeNode, key uint32) (Row, bool, error) {
	if node.IsLeaf() {
		leaf := node.(*LeafNode)
		idx := sort.Search(int(leaf.header.numCells), func(i int) bool {
			return leaf.cells[i].Key >= key
		})
		if idx < int(leaf.header.numCells) && leaf.cells[idx].Key == key {
			return leaf.cells[idx].Value, true, nil
		}
		return nil, false, nil
	}

	interior := node.(*InteriorNode)
	// find the first cell whose Key is greater than search key
	for _, cell := range interior.cells {
		if key < cell.Key {
			child, err := t.loadNode(cell.ChildPage)
			if err != nil {
				return nil, false, err
			}
			return t.searchNode(child, key)
		}
	}
	// otherwise descend to the rightmost pointer
	child, err := t.loadNode(interior.header.rightPointer)
	if err != nil {
		return nil, false, err
	}
	return t.searchNode(child, key)
}

// Insert adds key+row into the tree, splitting and promoting at the root if needed.
func (t *BTree) Insert(key uint32, row Row) error {
	// 1) load the root node
	root, err := t.loadNode(t.rootPage)
	if err != nil {
		return err
	}

	// 2) attempt to insert into root
	sibling, splitKey, didSplit := root.Insert(key, row)
	if !didSplit {
		// no split — just serialize the modified root
		p, err := t.bTreeMeta.Pager.GetPage(t.rootPage)
		if err != nil {
			return err
		}
		return root.Serialize(p)
	}

	// 3) root split: allocate a new root page
	newRootPage, err := t.AllocatePage()
	if err != nil {
		return err
	}

	// 4) clear the old root’s isRoot flag and re-serialize it
	if hdr := rootHeader(root); hdr != nil {
		hdr.isRoot = false
		oldP, _ := t.bTreeMeta.Pager.GetPage(root.Page())
		if err := root.Serialize(oldP); err != nil {
			return err
		}
	}

	// 5) serialize the new sibling (it must already carry its pageNum)
	sibP, _ := t.bTreeMeta.Pager.GetPage(sibling.Page())
	if err := sibling.Serialize(sibP); err != nil {
		return err
	}

	// 6) build and serialize the new interior root
	newRoot := &InteriorNode{
		header: baseHeader{
			pageNum:      newRootPage,
			isRoot:       true,
			parentPage:   0,
			numCells:     1,
			rightPointer: sibling.Page(),
		},
		cells: []InteriorCell{
			{ChildPage: root.Page(), Key: splitKey},
		},
	}
	nrP, _ := t.bTreeMeta.Pager.GetPage(newRootPage)
	if err := newRoot.Serialize(nrP); err != nil {
		return err
	}

	// 7) update tree’s root pointer in memory and on disk (meta page 0)
	t.rootPage = newRootPage
	mp, _ := t.bTreeMeta.Pager.GetPage(metaPageNum)
	binary.LittleEndian.PutUint32(mp.Data[metaRootOff:metaRootOff+4], newRootPage)
	mp.Dirty = true
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

// Seek repositions the cursor to the first key >= target key.
func (c *Cursor) Seek(target uint32) error {
	// Descend from root to leaf
	pgno := c.tree.rootPage
	for {
		node, err := c.tree.loadNode(pgno)
		if err != nil {
			return err
		}
		if node.IsLeaf() {
			leaf := node.(*LeafNode)
			idx := sort.Search(int(leaf.header.numCells), func(i int) bool {
				return leaf.cells[i].Key >= target
			})
			if idx >= int(leaf.header.numCells) {
				// Target doesn't exist in the tree - interior nodes guaranteed
				// we're in the correct leaf, so if target > all keys here, it doesn't exist
				c.valid = false
				return nil
			}
			c.leaf = leaf
			c.page = pgno
			c.idx = idx
			c.valid = true
			return nil
		}
		in := node.(*InteriorNode)
		// Binary search for the first cell with Key >= target
		idx := sort.Search(len(in.cells), func(i int) bool {
			return in.cells[i].Key >= target
		})
		var childPg uint32
		if idx < len(in.cells) {
			childPg = in.cells[idx].ChildPage
		} else {
			childPg = in.header.rightPointer
		}
		pgno = childPg
	}
}
