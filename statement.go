package main

import (
	"vqlite/table"
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type        StatementType
	RowToInsert table.Row
}
