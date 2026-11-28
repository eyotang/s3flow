package main

import (
	"os"

	"github.com/eyotang/s3flow/internal/objectcmd"
)

func main() { os.Exit(objectcmd.Run("cp", os.Args[1:], os.Stdout, os.Stderr)) }
