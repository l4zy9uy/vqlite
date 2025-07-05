package table

import (
	"encoding/binary"
	"fmt"
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
	tableMeta *TableMeta
}

func (n *LeafNode) Page() uint32 {
	return n.header.pageNum
}
func (n *LeafNode) IsLeaf() bool { return true }

// Insert a key/value into the sorted leaf.  If overflow, split.
func (n *LeafNode) Insert(key uint32, value Row) (BTreeNode, uint32, bool) {
	// find insertion index
	idx := sort.Search(int(n.header.numCells), func(i int) bool {
		return n.cells[i].Key >= key
	})
	// splice into slice
	n.cells = append(n.cells, LeafCell{})
	copy(n.cells[idx+1:], n.cells[idx:])
	n.cells[idx] = LeafCell{Key: key, Value: value}
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
		if err := SerializeRow(n.tableMeta, c.Value, p.Data[off:off+int(n.tableMeta.RowSize)]); err != nil {
			return fmt.Errorf("LeafNode.Serialize: %w", err)
		}
		off += int(n.tableMeta.RowSize)
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
		buf := make([]byte, n.tableMeta.RowSize)
		copy(buf, p.Data[off:off+int(n.tableMeta.RowSize)])
		off += int(n.tableMeta.RowSize)
		row, err := DeserializeRow(n.tableMeta, buf)
		if err != nil {
			return fmt.Errorf("LeafNode.Load: %w", err)
		}
		n.cells[i] = LeafCell{Key: key, Value: row}
	}
	return nil
}

// InteriorNode implements BTreeNode for interior pages.
type InteriorNode struct {
	header baseHeader
	cells  []InteriorCell
}

func (n *InteriorNode) Page() uint32 {
	return n.header.pageNum
}

func (n *InteriorNode) IsLeaf() bool { return false }

// Insert is a stub: you’ll hook in recursive descent and splitting here next.
func (n *InteriorNode) Insert(key uint32, row Row) (BTreeNode, uint32, bool) {
	// TODO: 1) find child index, 2) load child via BTree.loadNode,
	// 3) call child.Insert, 4) if split, splice into this.cells, split if needed.
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
