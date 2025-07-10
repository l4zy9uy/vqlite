package table

import (
	"fmt"
	"sort"

	"vqlite/pager"
)

const (
	maxCells = 12
)

// BTree manages the overall tree: root page, pager, and table meta.
type BTree struct {
	pager     *pager.Pager
	rootPage  uint32
	bTreeMeta *BTreeMeta
}

type BTreeMeta struct {
	Pager     *pager.Pager // for allocating pages, pageSize, etc.
	TableMeta *TableMeta   // schema, row sizes, max cells
}

// NewBTree opens or initializes a B+Tree on page 0.
// If the file is empty, it allocates page 0.
// You must pass in your TableMeta so leaf nodes know how to
// serialize/deserialize full rows.
//func NewBTree(p *pager.Pager, meta *TableMeta) (*BTree, error) {
//	return &BTree{
//		pager:    p,
//		rootPage: 0,
//		meta:     meta,
//	}, nil
//}

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
		p, err := t.pager.GetPage(t.rootPage)
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
		oldP, _ := t.pager.GetPage(root.Page())
		if err := root.Serialize(oldP); err != nil {
			return err
		}
	}

	// 5) serialize the new sibling (it must already carry its pageNum)
	sibP, _ := t.pager.GetPage(sibling.Page())
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
	nrP, _ := t.pager.GetPage(newRootPage)
	if err := newRoot.Serialize(nrP); err != nil {
		return err
	}

	// 7) update the tree’s root pointer
	t.rootPage = newRootPage
	return nil
}

// loadNode reads pageNum, inspects the first byte, and returns
// either a LeafNode (with meta) or InteriorNode.
func (t *BTree) loadNode(pageNum uint32) (BTreeNode, error) {
	p, err := t.pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}

	switch p.Data[0] {
	case nodeTypeLeaf:
		leaf := &LeafNode{bTreeMeta: t.bTreeMeta}
		leaf.header.pageNum = pageNum
		if err := leaf.Load(p); err != nil {
			return nil, err
		}
		// ensure pageNum is set after Load
		leaf.header.pageNum = pageNum
		return leaf, nil

	case nodeTypeInterior:
		inode := &InteriorNode{}
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
	return 0, nil
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
