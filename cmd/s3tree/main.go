package main

import (
	"os"

	"github.com/eyotang/s3flow/internal/tree"
)

func main() { os.Exit(tree.Run(os.Args[1:], os.Stdout, os.Stderr)) }
