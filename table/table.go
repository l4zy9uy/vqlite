package table

import (
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
	Pager   *pager.Pager
	Meta    *TableMeta
	NumRows uint32
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
	rowsPerPage := pager.PageSize / t.Meta.RowSize
	numPages := (t.NumRows + uint32(rowsPerPage) - 1) / uint32(rowsPerPage)

	for i := uint32(0); i < numPages; i++ {
		// flush the entire page
		if err := t.Pager.FlushPage(i, uint32(pager.PageSize)); err != nil {
			return err
		}
	}
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
	buf, err := t.rowSlot(t.NumRows)
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
	buf, err := t.rowSlot(rowNum)
	if err != nil {
		return nil, err
	}
	return DeserializeRow(t.Meta, buf)
}
