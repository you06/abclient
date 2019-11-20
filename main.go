package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/you06/doppelganger/executor"
	"github.com/you06/doppelganger/util"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var (
	printVersion bool
	dsn1        string
	dsn2        string
	printSchema bool
	clearDB     bool
	logPath     string
	reproduce   string
	stable      bool
	concurrency int
)

func init() {
	flag.BoolVar(&printVersion, "V", false, "print version")
	flag.BoolVar(&printSchema, "schema", false, "print schema and exit")
	flag.StringVar(&dsn1, "dsn1", "", "dsn1")
	flag.StringVar(&dsn2, "dsn2", "", "dsn2")
	flag.BoolVar(&clearDB, "clear", false, "drop all tables in target database and then start testing")
	flag.StringVar(&logPath, "log", "", "log path")
	flag.StringVar(&reproduce, "re", "", "reproduce from log, path:line, will execute to the line number, will not execute the given line")
	flag.BoolVar(&stable, "stable", false, "generate stable SQL without random or other env related expression")
	flag.IntVar(&concurrency, "concurrency", 1, "test concurrency")
}

func main() {
	flag.Parse()
	if printVersion {
		util.PrintInfo()
		os.Exit(0)
	}

	var (
		exec *executor.Executor
		err error
		opt = executor.Option{}
	)

	opt.Clear = clearDB
	opt.Log = logPath
	opt.Reproduce = reproduce
	opt.Stable = stable

	if dsn1 == "" {
		log.Fatalf("dsn1 can not be empty")
	} else if dsn2 == "" {
		exec, err = executor.New(dsn1, &opt)
	} else {
		exec, err = executor.NewABTest(dsn1, dsn2, &opt)
	}
	if err != nil {
		log.Fatalf("create executor error %v", errors.ErrorStack(err))
	}

	if printSchema {
		if err := exec.PrintSchema(); err != nil {
			log.Fatalf("print schema err %v", errors.ErrorStack(err))
		}
		os.Exit(0)
	}
	go exec.Start()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	log.Infof("Got signal %d to exit.", <-sc)
}
