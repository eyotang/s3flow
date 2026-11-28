package main

import (
	"os"

	"github.com/eyotang/s3flow/internal/objectcmd"
)

func main() { os.Exit(objectcmd.Run("presign", os.Args[1:], os.Stdout, os.Stderr)) }
