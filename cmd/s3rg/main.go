package main

import (
	"os"

	"github.com/eyotang/s3flow/internal/rg"
)

func main() { os.Exit(rg.Run(os.Args[1:], os.Stdout, os.Stderr)) }
