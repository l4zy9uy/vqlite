package column

type ColumnType int

const (
	ColumnTypeInt ColumnType = iota
	ColumnTypeText
)

type Column struct {
	Name      string
	Type      ColumnType
	Offset    uint32
	ByteSize  uint32
	MaxLength uint32
}

type Schema []Column
