package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"vqlite/column"
	"vqlite/pager"
)

type TableMeta struct {
	NumCols int
	Columns column.Schema
	RowSize uint32
}

type Table struct {
	Pager       *pager.Pager
	Meta        *TableMeta
	NumRows     uint32
	rootPageIdx uint32
}

// Cursor provides a way to iterate through rows in a Table.
type Cursor struct {
	table         *Table
	currentRowIdx uint32
	pageIdx       uint32
	cellIdx       uint32
}

func BuildTableMeta(schema column.Schema) (*TableMeta, error) {
	var metas []column.ColMeta
	var offset uint32 = 0

	for _, col := range schema {
		switch col.Type {
		case column.ColumnTypeInt:
			metas = append(metas, column.ColMeta{
				Name:      col.Name,
				Type:      column.ColumnTypeInt,
				Offset:    offset,
				ByteSize:  4,
				MaxLength: 0,
			})
			offset += 4

		case column.ColumnTypeText:
			if col.MaxLength == 0 {
				return nil, fmt.Errorf("TEXT column %q must have MaxLength>0", col.Name)
			}
			metas = append(metas, column.ColMeta{
				Name:      col.Name,
				Type:      column.ColumnTypeText,
				Offset:    offset,
				ByteSize:  col.MaxLength,
				MaxLength: col.MaxLength,
			})
			offset += col.MaxLength

		default:
			return nil, fmt.Errorf("unsupported column type for %q", col.Name)
		}
	}

	totalSize := offset
	if totalSize == 0 {
		return nil, errors.New("schema must have at least one column")
	}

	return &TableMeta{
		NumCols: len(schema),
		Columns: metas,
		RowSize: totalSize,
	}, nil
}

// OpenTable creates a Table backed by filename and computes NumRows = fileLength / PageSize.
func OpenTable(filename string, schema column.Schema) (*Table, error) {
	pg, err := pager.OpenPager(filename)
	if err != nil {
		return nil, err
	}
	meta, err := BuildTableMeta(schema)
	if err != nil {
		return nil, err
	}
	numRows := uint32(pg.FileLength / uint64(meta.RowSize))
	return &Table{
		Pager:   pg,
		Meta:    meta,
		NumRows: numRows,
	}, nil
}

func (t *Table) Close() error {
	// 1) How many full pages do we have?
	numRowsPerPage := pager.PageSize / t.Meta.RowSize
	fullPages := t.NumRows / numRowsPerPage

	// 2) Flush each fully‐occupied page (PageSize bytes).
	for i := uint32(0); i < fullPages; i++ {
		if err := t.Pager.FlushPage(i, pager.PageSize); err != nil {
			return fmt.Errorf("Close: flushing full page %d: %w", i, err)
		}
	}

	// 3) Flush the *partial* page, if any rows remain.
	leftover := t.NumRows % numRowsPerPage
	if leftover > 0 {
		pageNum := fullPages
		// Write only the bytes that actually hold rows:
		sizeToWrite := leftover * t.Meta.RowSize
		if err := t.Pager.FlushPage(pageNum, sizeToWrite); err != nil {
			return fmt.Errorf("Close: flushing partial page %d: %w", pageNum, err)
		}
	}

	// 4) Finally, close the underlying file
	return t.Pager.File.Close()
}

func (t *Table) rowSlot(rowNum uint32) ([]byte, error) {
	numRowsPerPage := pager.PageSize / t.Meta.RowSize
	pageNum := rowNum / numRowsPerPage
	if pageNum >= pager.TableMaxPages {
		return nil, fmt.Errorf("rowSlot: page %d out of bounds", pageNum)
	}
	pg, err := t.Pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	rowOffset := rowNum % (pager.PageSize / t.Meta.RowSize)
	byteOffset := rowOffset * t.Meta.RowSize
	return pg.Data[byteOffset : byteOffset+t.Meta.RowSize], nil
}

func (t *Table) InsertRow(row Row) error {
	// 1) Get the target slot
	cur, err := t.CursorAt(t.NumRows)
	if err != nil {
		return err
	}

	// 2) Get the raw slot bytes:
	buf, err := cur.GetValue()
	if err != nil {
		return err
	}

	if err := SerializeRow(t.Meta, row, buf); err != nil {
		return err
	}
	// 3) Mark as “dirty” and increment count
	//    (we’ll flush on Close; you could also flush here page-by-page)
	t.NumRows++
	return nil
}

func (t *Table) GetRow(rowNum uint32) (Row, error) {
	cursor := t.StartCursor()
	for i := uint32(0); i < rowNum; i++ {
		cursor.Advance()
	}
	buf, err := cursor.GetValue()
	if err != nil {
		return nil, err
	}
	return DeserializeRow(t.Meta, buf)
}

// StartCursor returns a new Cursor positioned at the first row of the table.
// If the table is empty, End() will be true immediately.
func (t *Table) StartCursor() *Cursor {
	rootNodePage, _ := t.Pager.GetPage(t.rootPageIdx)
	cellIdx := binary.LittleEndian.Uint32(rootNodePage.Data[NodeTypeSize+IsRootSize+ParentPointerSize : NodeTypeSize+IsRootSize+ParentPointerSize+LeafNodeNumCellsSize])
	return &Cursor{
		table:         t,
		currentRowIdx: 0,
		pageIdx:       t.rootPageIdx,
		cellIdx:       cellIdx,
	}
}

func (t *Table) EndCursor() *Cursor {
	return &Cursor{
		table:         t,
		currentRowIdx: t.NumRows,
	}
}

func (c *Cursor) Advance() {
	if c.currentRowIdx >= c.table.NumRows {
		return
	}
	c.currentRowIdx++
}

func (c *Cursor) GetValue() ([]byte, error) {
	if c.currentRowIdx > c.table.NumRows {
		return nil, fmt.Errorf("Cursor.Value: index %d out of bounds", c.currentRowIdx)
	}
	// Calculate page and offset directly
	pageIdx := c.pageIdx
	page, err := c.table.Pager.GetPage(pageIdx)
	if err != nil {
		return nil, fmt.Errorf("Cursor.Value: failed to load page %d: %w", pageIdx, err)
	}
	offset := c.currentRowIdx * c.table.Meta.RowSize
	// Return the slice of bytes for this row
	return page.Data[offset : offset+c.table.Meta.RowSize], nil
}

// Seek the cursor to the given absolute row index (0-based).
// rowIdx may be equal to t.NumRows if you want to append a new row.
// Returns an error if rowIdx > t.NumRows.
func (t *Table) CursorAt(rowIdx uint32) (*Cursor, error) {
	if rowIdx > t.NumRows {
		return nil, fmt.Errorf("CursorAt: row index %d out of bounds (NumRows=%d)", rowIdx, t.NumRows)
	}
	return &Cursor{
		table:         t,
		currentRowIdx: rowIdx,
		// we no longer need endOfTable for writes; you can check rowIdx==NumRows yourself
	}, nil
}
