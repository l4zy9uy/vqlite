package table

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"vqlite/pager"
)

const (
	minCells = maxCells / 2 // minimum cells to avoid underflow

	// on-disk header layout
	nodeTypeLeaf     = 1
	nodeTypeInterior = 0
	// type (1) + isRoot (1) + parentPage (4) + numCells (4) + rightPointer (4)
	headerSize = 1 + 1 + 4 + 4 + 4
)

// BTreeNode is the interface for any node in the B+-tree.
type BTreeNode interface {
	Page() uint32

	// IsLeaf tells us whether this is a leaf or interior node.
	IsLeaf() bool

	// Insert tries to insert the given key and value
	// into this node.  If the node overflows, it returns (newNode, splitKey, true).
	// Otherwise (nil, 0, false).
	Insert(c *Cursor, key uint32, value Row) (newNode BTreeNode, splitKey uint32, split bool)

	// Delete tries to delete the given key from this node.
	// Returns (found, needsRebalance) where found indicates if key was deleted
	// and needsRebalance indicates if this node needs rebalancing due to underflow.
	Delete(key uint32) (found bool, needsRebalance bool)

	// Serialize writes the node back to its on-disk page.
	Serialize(p *pager.Page) error

	// Load populates this node’s in-memory fields from its on-disk page.
	Load(p *pager.Page) error

	// Search for a key recursively, returning (cmp, idx, err)
	Search(c *Cursor, key uint32) (int, error)
}

type LeafCell struct {
	Key   uint32
	Value Row
}
type InteriorCell struct {
	ChildPage uint32
	Key       uint32
}

// LeafNode implements BTreeNode for leaf pages.
type LeafNode struct {
	header    baseHeader
	cells     []LeafCell
	bTreeMeta *BTreeMeta
}

func (n *LeafNode) Page() uint32 {
	return n.header.pageNum
}
func (n *LeafNode) IsLeaf() bool { return true }

// NewLeafNode allocates a fresh page and returns a new leaf node
func NewLeafNode(meta *BTreeMeta, isRoot bool) (*LeafNode, error) {
	// 1) Allocate a fresh page (from free-list or by extending the file)
	pgno, err := meta.Pager.AllocatePage()
	if err != nil {
		return nil, fmt.Errorf("NewLeafNode: could not allocate page: %w", err)
	}

	// 2) Build the in-memory node with that page number
	n := &LeafNode{
		bTreeMeta: meta,
		header: baseHeader{
			pageNum:      pgno,
			isRoot:       isRoot,
			parentPage:   0,
			numCells:     0,
			rightPointer: 0,
		},
		cells: make([]LeafCell, 0, maxCells),
	}

	// 3) Mark the page dirty so on next flush it will be zeroed & initialized
	pg, err := meta.Pager.GetPage(pgno)
	if err != nil {
		return nil, fmt.Errorf("NewLeafNode: could not get page: %w", err)
	}
	pg.Dirty = true

	return n, nil
}

func (n *LeafNode) Search(c *Cursor, key uint32) (int, error) {
	// 1) Binary‐search in this leaf
	idx := sort.Search(len(n.cells), func(i int) bool {
		return n.cells[i].Key >= key
	})

	// 2) Update the cursor
	c.leaf = n                // this leaf
	c.page = n.header.pageNum // its page number
	c.idx = idx               // slot index
	// 3) Decide exact vs before vs after
	if idx < len(n.cells) && n.cells[idx].Key == key {
		c.valid = true
		return 0, nil
	}
	c.valid = false
	if idx == len(n.cells) {
		return +1, nil
	}
	return -1, nil
}

// Insert uses c.idx (positioned by Search) to insert or update in-place. On overflow, splits and updates cursor.
func (n *LeafNode) Insert(c *Cursor, key uint32, value Row) (BTreeNode, uint32, bool) {
	idx := c.idx
	// update existing
	if idx < len(n.cells) && n.cells[idx].Key == key {
		n.cells[idx].Value = value
		n.header.numCells = uint32(len(n.cells))
		return nil, 0, false
	}
	// clamp insertion index
	if idx > len(n.cells) {
		idx = len(n.cells)
	}
	// insert new cell
	n.cells = slices.Insert(n.cells, idx, LeafCell{Key: key, Value: value})
	n.header.numCells = uint32(len(n.cells))
	// no split
	if len(n.cells) <= maxCells {
		c.idx = idx
		return nil, 0, false
	}
	// split leaf
	sib, _ := NewLeafNode(n.bTreeMeta, false)
	sib.header.parentPage = n.header.parentPage
	sib.header.rightPointer = n.header.rightPointer
	mid := len(n.cells) / 2
	sib.cells = append(sib.cells, n.cells[mid:]...)
	sib.header.numCells = uint32(len(sib.cells))
	n.cells = n.cells[:mid]
	n.header.numCells = uint32(len(n.cells))
	n.header.rightPointer = sib.Page()
	// determine new cursor position
	if idx >= mid {
		c.leaf = sib
		c.idx = idx - mid
	} else {
		c.idx = idx
	}
	splitKey := sib.cells[0].Key
	return sib, splitKey, true
}

// Delete removes the given key from the leaf node.
// Returns (found, needsRebalance) where found indicates if key was deleted
// and needsRebalance indicates if this node needs rebalancing due to underflow.
func (n *LeafNode) Delete(key uint32) (found bool, needsRebalance bool) {
	// Find the key using binary search
	idx := sort.Search(int(n.header.numCells), func(i int) bool {
		return n.cells[i].Key >= key
	})

	// Check if we found the exact key
	if idx >= int(n.header.numCells) || n.cells[idx].Key != key {
		return false, false // Key not found
	}

	// Remove the cell at idx
	n.cells = append(n.cells[:idx], n.cells[idx+1:]...)
	n.header.numCells = uint32(len(n.cells))

	// For simplicity, we don't implement full rebalancing here
	// Just return true for found, false for needsRebalance
	// This is a simplified deletion that works for basic cases
	return true, false
}

// Serialize writes the header + all cells to p.Data.
// Each cell is: [ key:uint32 | serialized row (meta.RowSize bytes) ].
// Uses table.SerializeRow from row.go :contentReference[oaicite:0]{index=0}.
func (n *LeafNode) Serialize(p *pager.Page) error {
	// zero-out
	for i := range p.Data {
		p.Data[i] = 0
	}
	// header
	n.header.writeTo(p.Data[:headerSize], nodeTypeLeaf)
	// cells
	off := headerSize
	for _, c := range n.cells {
		binary.LittleEndian.PutUint32(p.Data[off:off+4], c.Key)
		off += 4
		// serialize full row
		if err := SerializeRow(n.bTreeMeta.TableMeta, c.Value, p.Data[off:off+int(n.bTreeMeta.TableMeta.RowSize)]); err != nil {
			return fmt.Errorf("LeafNode.Serialize: %w", err)
		}
		off += int(n.bTreeMeta.TableMeta.RowSize)
	}
	return nil
}

func (n *LeafNode) Load(p *pager.Page) error {
	if p.Data[0] != nodeTypeLeaf {
		return fmt.Errorf("LeafNode.Load: not a leaf (type=%d)", p.Data[0])
	}
	n.header.readFrom(p.Data[:headerSize])
	cnt := int(n.header.numCells)
	n.cells = make([]LeafCell, cnt)
	off := headerSize
	for i := 0; i < cnt; i++ {
		key := binary.LittleEndian.Uint32(p.Data[off : off+4])
		off += 4
		buf := make([]byte, n.bTreeMeta.TableMeta.RowSize)
		copy(buf, p.Data[off:off+int(n.bTreeMeta.TableMeta.RowSize)])
		off += int(n.bTreeMeta.TableMeta.RowSize)
		row, err := DeserializeRow(n.bTreeMeta.TableMeta, buf)
		if err != nil {
			return fmt.Errorf("LeafNode.Load: %w", err)
		}
		n.cells[i] = LeafCell{Key: key, Value: row}
	}
	return nil
}

// InteriorNode implements BTreeNode for interior pages.
type InteriorNode struct {
	header    baseHeader
	cells     []InteriorCell
	bTreeMeta *BTreeMeta
}

func (n *InteriorNode) Page() uint32 {
	return n.header.pageNum
}

func (n *InteriorNode) IsLeaf() bool { return false }

// NewInteriorNode allocates a fresh page (like NewLeafNode) and returns an
// empty interior node. The caller should set header.rightPointer and/or cells
// before serialization if needed.
func NewInteriorNode(meta *BTreeMeta, isRoot bool) (*InteriorNode, error) {
	// 1) allocate new page
	pgno, err := meta.Pager.AllocatePage()
	if err != nil {
		return nil, fmt.Errorf("NewInteriorNode: could not allocate page: %w", err)
	}

	n := &InteriorNode{
		bTreeMeta: meta,
		header: baseHeader{
			pageNum:      pgno,
			isRoot:       isRoot,
			parentPage:   0,
			numCells:     0,
			rightPointer: 0,
		},
		cells: make([]InteriorCell, 0, maxCells),
	}

	// mark page dirty so it will be zeroed/serialized later
	pg, err := meta.Pager.GetPage(pgno)
	if err != nil {
		return nil, fmt.Errorf("NewInteriorNode: could not get page: %w", err)
	}
	pg.Dirty = true

	return n, nil
}

// Insert descends to child, recurses, and splices on split; splits this node if needed.
// Cursor is accepted for API consistency but only used at leaf level.
func (n *InteriorNode) Insert(c *Cursor, key uint32, value Row) (BTreeNode, uint32, bool) {
	// find branch index
	i := sort.Search(len(n.cells), func(i int) bool { return n.cells[i].Key >= key })
	var childPg uint32
	if i < len(n.cells) {
		childPg = n.cells[i].ChildPage
	} else {
		childPg = n.header.rightPointer
	}

	// load child node
	page, _ := n.bTreeMeta.Pager.GetPage(childPg)
	var child BTreeNode
	if page.Data[0] == nodeTypeLeaf {
		leaf := &LeafNode{bTreeMeta: n.bTreeMeta}
		leaf.header.pageNum = childPg
		leaf.Load(page)
		child = leaf
	} else {
		in := &InteriorNode{bTreeMeta: n.bTreeMeta}
		in.header.pageNum = childPg
		in.Load(page)
		child = in
	}

	// recurse
	sib, splitKey, didSplit := child.Insert(c, key, value)
	if !didSplit {
		return nil, 0, false
	}

	// splice in new child pointer
	n.cells = slices.Insert(n.cells, i, InteriorCell{ChildPage: sib.Page(), Key: splitKey})
	n.header.numCells = uint32(len(n.cells))

	// if no overflow, serialize
	if len(n.cells) <= maxCells {
		p, _ := n.bTreeMeta.Pager.GetPage(n.Page())
		n.Serialize(p)
		return nil, 0, false
	}

	// split interior node
	sibInt, _ := NewInteriorNode(n.bTreeMeta, false)
	sibInt.header.parentPage = n.header.parentPage
	mid := len(n.cells) / 2
	med := n.cells[mid]

	sibInt.cells = append(sibInt.cells, n.cells[mid+1:]...)
	sibInt.header.numCells = uint32(len(sibInt.cells))
	sibInt.header.rightPointer = n.header.rightPointer

	n.cells = n.cells[:mid]
	n.header.numCells = uint32(len(n.cells))
	n.header.rightPointer = med.ChildPage

	// serialize both halves
	if pN, _ := n.bTreeMeta.Pager.GetPage(n.Page()); pN != nil {
		n.Serialize(pN)
	}
	if pS, _ := n.bTreeMeta.Pager.GetPage(sibInt.Page()); pS != nil {
		sibInt.Serialize(pS)
	}
	return sibInt, med.Key, true
}

// Delete removes the given key from the interior node by recursively
// descending to the appropriate child.
// Returns (found, needsRebalance) where found indicates if key was deleted
// and needsRebalance indicates if this node needs rebalancing due to underflow.
func (n *InteriorNode) Delete(key uint32) (found bool, needsRebalance bool) {
	// Find the appropriate child to descend to
	i := sort.Search(len(n.cells), func(i int) bool {
		return n.cells[i].Key >= key
	})

	var childPg uint32
	if i < len(n.cells) {
		childPg = n.cells[i].ChildPage
	} else {
		childPg = n.header.rightPointer
	}

	// Load the child node
	p, err := n.bTreeMeta.Pager.GetPage(childPg)
	if err != nil {
		return false, false // Error loading child
	}

	var child BTreeNode
	if p.Data[0] == nodeTypeLeaf {
		leaf := &LeafNode{bTreeMeta: n.bTreeMeta}
		leaf.header.pageNum = childPg
		if err := leaf.Load(p); err != nil {
			return false, false
		}
		child = leaf
	} else {
		interior := &InteriorNode{bTreeMeta: n.bTreeMeta}
		interior.header.pageNum = childPg
		if err := interior.Load(p); err != nil {
			return false, false
		}
		child = interior
	}

	// Recursively delete from child
	found, _ = child.Delete(key)
	if !found {
		return false, false // Key not found in subtree
	}

	// Serialize the modified child back to disk
	if err := child.Serialize(p); err != nil {
		return false, false
	}

	// For simplicity, we don't implement full rebalancing here
	// Just return that deletion was successful
	return true, false
}

// Serialize writes header + each InteriorCell ([ childPage:uint32 | key:uint32 ]).
func (n *InteriorNode) Serialize(p *pager.Page) error {
	for i := range p.Data {
		p.Data[i] = 0
	}
	n.header.writeTo(p.Data[:headerSize], nodeTypeInterior)
	off := headerSize
	for _, c := range n.cells {
		binary.LittleEndian.PutUint32(p.Data[off:off+4], c.ChildPage)
		binary.LittleEndian.PutUint32(p.Data[off+4:off+8], c.Key)
		off += 8
	}
	return nil
}

// Load reads header + cells for an interior page.
func (n *InteriorNode) Load(p *pager.Page) error {
	if p.Data[0] != nodeTypeInterior {
		return fmt.Errorf("InteriorNode.Load: not interior (type=%d)", p.Data[0])
	}
	n.header.readFrom(p.Data[:headerSize])
	cnt := int(n.header.numCells)
	n.cells = make([]InteriorCell, cnt)
	off := headerSize
	for i := 0; i < cnt; i++ {
		child := binary.LittleEndian.Uint32(p.Data[off : off+4])
		key := binary.LittleEndian.Uint32(p.Data[off+4 : off+8])
		off += 8
		n.cells[i] = InteriorCell{ChildPage: child, Key: key}
	}
	return nil
}

// Search on an interior page: pick the correct child, load it, and recurse.
// Returns –1/0/+1 from the eventual leaf, and updates the same *Cursor.
func (n *InteriorNode) Search(c *Cursor, key uint32) (int, error) {
	// 1) Find the first cell whose Key > search key
	childIdx := sort.Search(len(n.cells), func(i int) bool {
		return n.cells[i].Key >= key
	})

	// 2) Choose the child page pointer
	var childPg uint32
	if childIdx < len(n.cells) {
		childPg = n.cells[childIdx].ChildPage
	} else {
		childPg = n.header.rightPointer
	}

	// 3) Load that child node
	node, err := c.tree.loadNode(childPg)
	if err != nil {
		return 0, err
	}

	return node.Search(c, key)
}
