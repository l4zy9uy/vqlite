package table

import "encoding/binary"

// baseHeader holds the header fields common to both node types.
type baseHeader struct {
	pageNum      uint32
	isRoot       bool
	parentPage   uint32
	numCells     uint32
	rightPointer uint32 // for leaf: next leaf; for interior: rightmost child
}

func (h *baseHeader) Page() uint32     { return h.pageNum }
func (h *baseHeader) NumCells() uint32 { return h.numCells }

func (h *baseHeader) writeTo(buf []byte, ntype byte) {
	buf[0] = ntype
	if h.isRoot {
		buf[1] = 1
	} else {
		buf[1] = 0
	}
	binary.LittleEndian.PutUint32(buf[2:6], h.parentPage)
	binary.LittleEndian.PutUint32(buf[6:10], h.numCells)
	binary.LittleEndian.PutUint32(buf[10:14], h.rightPointer)
}

func (h *baseHeader) readFrom(buf []byte) {
	h.isRoot = buf[1] == 1
	h.parentPage = binary.LittleEndian.Uint32(buf[2:6])
	h.numCells = binary.LittleEndian.Uint32(buf[6:10])
	h.rightPointer = binary.LittleEndian.Uint32(buf[10:14])
}
