package main

import (
	"github.com/eyotang/s3flow/internal/unzip"
	"os"
)

// main runs the standalone ZIP extraction command.
func main() { os.Exit(unzip.Run(os.Args[1:], os.Stdout, os.Stderr)) }
