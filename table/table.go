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

// Table is now a pure catalog entry, mirroring SQLite‘s design.  It carries
// only schema information (Meta) and the root page of its primary B-tree.  It
// no longer owns a Pager or Row counters – those are managed by BTree and
// higher-level engine layers.
type Table struct {
	Name     string
	Meta     *TableMeta
	RootPage uint32

	// NOTE: The fields below remain temporarily so existing helper functions
	// compile until we finish migrating InsertRow/GetRow to the B-tree layer.
	// They will be removed in a subsequent commit.
	Pager   *pager.Pager // TODO: delete after migration
	NumRows uint32       // cached only by old InsertRow implementation
}

// Legacy Cursor & flat-row access removed; iteration will be provided by the
// B-tree layer’s own cursor implementation.

func BuildTableMeta(schema column.Schema) (*TableMeta, error) {
	var metas []column.Column
	var offset uint32 = 0

	for _, col := range schema {
		switch col.Type {
		case column.ColumnTypeInt:
			metas = append(metas, column.Column{
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
			metas = append(metas, column.Column{
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
func OpenTable(filename string, schema column.Schema) (*Table, *pager.Pager, error) {
	pg, err := pager.OpenPager(filename)
	if err != nil {
		return nil, nil, err
	}
	meta, err := BuildTableMeta(schema)
	if err != nil {
		return nil, nil, err
	}
	numRows := uint32(pg.NumPages*pager.PageSize) / meta.RowSize
	return &Table{
		Name:     filename, // Assuming filename is the table name for now
		Meta:     meta,
		RootPage: 0, // Placeholder, will be updated by BTree
		Pager:    pg,
		NumRows:  numRows,
	}, pg, nil
}

// All row-level operations (insert/search/scan) have moved to the B-tree layer.
