package table

import (
	"encoding/binary"
	"fmt"
	"strings"
	"vqlite/column"
)

type Row []interface{}

func SerializeRow(meta *TableMeta, row Row, dst []byte) error {
	if uint32(len(dst)) != meta.RowSize {
		return fmt.Errorf("SerializeRow: dst length %d, expected %d", len(dst), meta.RowSize)
	}
	if len(row) != meta.NumCols {
		return fmt.Errorf("SerializeRow: row has %d columns, expected %d", len(row), meta.NumCols)
	}

	// Zero out the entire destination (in case of leftover bytes).
	for i := range dst {
		dst[i] = 0
	}

	for i, colMeta := range meta.Columns {
		base := colMeta.Offset
		switch colMeta.Type {
		case column.ColumnTypeInt:
			val, ok := row[i].(uint32)
			if !ok {
				return fmt.Errorf("SerializeRow: column %q expects uint32, got %T", colMeta.Name, row[i])
			}
			binary.LittleEndian.PutUint32(dst[base:base+4], val)

		case column.ColumnTypeText:
			s, ok := row[i].(string)
			if !ok {
				return fmt.Errorf("SerializeRow: column %q expects string, got %T", colMeta.Name, row[i])
			}
			bytes := []byte(s)
			if uint32(len(bytes)) > colMeta.MaxLength {
				copy(dst[base:base+colMeta.MaxLength], bytes[:colMeta.MaxLength])
			} else {
				copy(dst[base:base+uint32(len(bytes))], bytes)
			}
		}
	}

	return nil
}

func DeserializeRow(meta *TableMeta, src []byte) (Row, error) {
	if uint32(len(src)) != meta.RowSize {
		return nil, fmt.Errorf("DeserializeRow: src length %d, expected %d", len(src), meta.RowSize)
	}

	row := make(Row, meta.NumCols)
	for i, colMeta := range meta.Columns {
		base := colMeta.Offset
		switch colMeta.Type {
		case column.ColumnTypeInt:
			val := binary.LittleEndian.Uint32(src[base : base+4])
			row[i] = val

		case column.ColumnTypeText:
			raw := src[base : base+colMeta.ByteSize]
			// Trim any trailing zero bytes so we get the original string.
			str := string(raw)
			str = strings.TrimRight(str, "\x00")
			row[i] = str
		}
	}

	return row, nil
}
