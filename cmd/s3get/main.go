package main

import (
	"github.com/eyotang/s3flow/internal/get"
	"os"
)

func main() { os.Exit(get.Run(os.Args[1:], os.Stdout, os.Stderr)) }
