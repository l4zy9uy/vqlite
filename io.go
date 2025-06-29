package main

import (
	"bufio"
	"fmt"
	"strings"
)

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader) (string, error) {
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(input), nil
}
