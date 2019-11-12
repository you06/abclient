package main

import (
	"testing"
	"github.com/you06/sqlsmith-client/executor"
)

func TestMain(t *testing.T) {
	dsn1 := "root:@tcp(172.16.5.6:33306)/sqlsmith"
	exec, _ := executor.New(dsn1)
	exec.Start()
}
