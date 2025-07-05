package table

import (
	"unsafe"
	"vqlite/pager"
)

const (
	// Common Node Header Layout
	NodeTypeSize        = unsafe.Sizeof(uint8(0))
	NodeTypeOffset      = 0
	IsRootSize          = unsafe.Sizeof(uint8(0))
	IsRootOffset        = NodeTypeOffset + NodeTypeSize
	ParentPointerSize   = unsafe.Sizeof(uint32(0))
	ParentPointerOffset = IsRootOffset + IsRootSize
	// size of the common header (type + isRoot + parentPointer)
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize

	// Leaf Node Header Layout
	LeafNodeNumCellsSize   = unsafe.Sizeof(uint32(0))
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize     = uint32(CommonNodeHeaderSize + LeafNodeNumCellsSize)

	// Leaf Node Body Layout (key + value)
	LeafNodeKeySize   = 4
	LeafNodeKeyOffset = 0
)

func LeafCellSize(rowSize uint32) uint32 {
	return LeafNodeKeySize + rowSize
}

// LeafSpaceForCells returns available bytes for cells in a page.
func LeafSpaceForCells() uint32 {
	return pager.PageSize - LeafNodeHeaderSize
}

// LeafMaxCells returns how many cells fit in a page for a given row size.
func LeafMaxCells(rowSize uint32) uint32 {
	return LeafSpaceForCells() / LeafCellSize(rowSize)
}
