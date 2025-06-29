package main

import "strings"

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognizedCommand
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
)

const RowsPerPageGuess = 32

// handleMetaCommand checks if the input line is “.exit”. If so, it terminates.
func handleMetaCommand(line string) MetaCommandResult {
	if strings.TrimSpace(line) == ".exit" {
		return MetaCommandSuccess
	}
	return MetaCommandUnrecognizedCommand
}
