package main

import (
	"github.com/eyotang/s3flow/internal/push"
	"os"
)

func main() { os.Exit(push.Run(os.Args[1:], os.Stdout, os.Stderr)) }
