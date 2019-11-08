package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/you06/sqlsmith-client/util"
	"github.com/ngaut/log"
)

var (
	cfg          *config.Config
	printVersion bool
	dsn1         string
	dsn2         string
	testMode     bool
	taskID       int
)

func init() {
	flag.BoolVar(&printVersion, "V", false, "print version")
	flag.StringVar(&dsn1, "dsn1", "", "dsn1")
	flag.StringVar(&dsn2, "dsn2", "", "dsn2")
}

func main() {
	flag.Parse()
	if printVersion {
		util.PrintInfo()
		os.Exit(0)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	log.Infof("Got signal %d to exit.", sig)
}
