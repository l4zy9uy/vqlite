package table

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"vqlite/pager"
)

const (
	// on-disk header layout
	nodeTypeLeaf     = 1
	nodeTypeInterior = 0
	// type (1) + isRoot (1) + parentPage (4) + numCells (4) + rightPointer (4)
	headerSize = 1 + 1 + 4 + 4 + 4
)

// BTreeNode is the interface for any node in the B+-tree.
type BTreeNode interface {
	Page() uint32

	// IsLeaf() tells us whether this is a leaf or interior node.
	IsLeaf() bool

	// Insert(key, value) tries to insert the given key and value
	// into this node.  If the node overflows, it returns (newNode, splitKey, true).
	// Otherwise (nil, 0, false).
	Insert(key uint32, value Row) (newNode BTreeNode, splitKey uint32, split bool)

	// Serialize writes the node back to its on-disk page.
	Serialize(p *pager.Page) error

	// Load populates this node’s in-memory fields from its on-disk page.
	Load(p *pager.Page) error
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

// Insert a key/value into the sorted leaf.  If overflow, split.
func (n *LeafNode) Insert(key uint32, value Row) (BTreeNode, uint32, bool) {
	// find insertion index
	idx := sort.Search(int(n.header.numCells), func(i int) bool {
		return n.cells[i].Key >= key
	})

	newCell := LeafCell{Key: key, Value: value}
	n.cells = slices.Insert(n.cells, idx, newCell)

	if len(n.cells) > maxCells {
		// 2) Build the sibling with that page number:
		sibling, err := NewLeafNode(n.bTreeMeta, false)
		if err != nil {
			panic(err)
		}
		//      copy the “right half” of the cells into it:
		mid := len(n.cells) / 2
		sibling.cells = append(sibling.cells, n.cells[mid:]...)
		sibling.header.numCells = uint32(len(sibling.cells))
		sibling.header.rightPointer = n.header.rightPointer

		// 3) Trim the original:
		n.cells = n.cells[:mid]
		n.header.numCells = uint32(len(n.cells))
		n.header.rightPointer = sibling.Page()

		// 4) Return sibling (with its pageNum set!), the splitKey, and true:
		return sibling, sibling.cells[0].Key, true
	}

	n.header.numCells = uint32(len(n.cells))
	return nil, 0, false
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

// Insert is a stub: you’ll hook in recursive descent and splitting here next.
// InteriorNode.Insert inserts into an interior. Propagates splits upward.
func (n *InteriorNode) Insert(key uint32, value Row) (BTreeNode, uint32, bool) {
	// 1) find child index
	i := sort.Search(len(n.cells), func(i int) bool { return n.cells[i].Key >= key })
	// choose the child page
	var childPg uint32
	if i < len(n.cells) {
		childPg = n.cells[i].ChildPage
	} else {
		childPg = n.header.rightPointer
	}
	// 2) load child page
	p, _ := n.bTreeMeta.Pager.GetPage(childPg)
	// 3) instantiate child node struct _without allocating new pages_
	var child BTreeNode
	if p.Data[0] == nodeTypeLeaf {
		// Use a zero LeafNode bound to the existing page
		leaf := &LeafNode{bTreeMeta: n.bTreeMeta}
		leaf.header.pageNum = childPg
		child = leaf
	} else {
		in := &InteriorNode{bTreeMeta: n.bTreeMeta}
		in.header.pageNum = childPg
		child = in
	}
	// populate it from disk
	if err := child.Load(p); err != nil {
		return nil, 0, false // propagate error silently for now
	}
	// 4) recurse
	sib, splitKey, didSplit := child.Insert(key, value)
	if !didSplit {
		// Child accepted insert with no split: serialize child back and we’re done.
		child.Serialize(p)
		return nil, 0, false
	}
	// 5) splice new cell
	// Child split – insert separator key+pointer into this node
	n.cells = slices.Insert(n.cells, i, InteriorCell{ChildPage: sib.Page(), Key: splitKey})
	n.header.numCells = uint32(len(n.cells))
	// 6) handle interior overflow
	if len(n.cells) > maxCells {
		// This interior now overflows – split it as well
		sibInt, err := NewInteriorNode(n.bTreeMeta, false)
		if err != nil {
			panic(err)
		}
		mid := len(n.cells) / 2
		// median cell to push up
		med := n.cells[mid]
		// right half -> sibling
		sibInt.cells = append(sibInt.cells, n.cells[mid+1:]...)
		sibInt.header.numCells = uint32(len(sibInt.cells))
		sibInt.header.rightPointer = n.header.rightPointer
		// left keep
		n.cells = n.cells[:mid]
		n.header.numCells = uint32(len(n.cells))
		n.header.rightPointer = med.ChildPage
		// Serialize left (n) and right (sibInt) nodes
		if pgN, _ := n.bTreeMeta.Pager.GetPage(n.Page()); pgN != nil {
			n.Serialize(pgN)
		}
		if pgS, _ := n.bTreeMeta.Pager.GetPage(sibInt.Page()); pgS != nil {
			sibInt.Serialize(pgS)
		}

		// propagate split upward
		return sibInt, med.Key, true
	}
	// No overflow after inserting separator. Serialize self and child/sibling.
	if pgChild, _ := n.bTreeMeta.Pager.GetPage(child.Page()); pgChild != nil {
		child.Serialize(pgChild)
	}
	if pgSib, _ := n.bTreeMeta.Pager.GetPage(sib.Page()); pgSib != nil {
		sib.Serialize(pgSib)
	}
	if pgSelf, _ := n.bTreeMeta.Pager.GetPage(n.Page()); pgSelf != nil {
		n.Serialize(pgSelf)
	}

	// Don’t propagate – our node did not split.
	return nil, 0, false
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
