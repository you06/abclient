package main

import (
	"github.com/ngaut/log"
	"github.com/juju/errors"
	"github.com/you06/doppelganger/connection"
)

const (
	dsn = "root@tcp(172.17.0.1:4000)/repro"
)

func main() {
	for i := 0; i < 100000; i++ {
		test()
		if i % 1000 == 0 {
			log.Infof("try %d times \n", i)
		}
	}
}

func test() {
	conn, err := connection.New(dsn, &connection.Option{
		Mute: true,
	})
	if err != nil {
		log.Fatalf("init connection error %+v\n", errors.ErrorStack(err))
	}

	err = conn.Begin()
	if err != nil {
		log.Fatalf("init connection fatal %+v\n", errors.ErrorStack(err))
	} else {
		// log.Info("begin")
	}

	err = conn.Exec("INSERT INTO t(c) VALUES(1)")
	if err != nil {
		log.Fatalf("exec fatal %+v\n", errors.ErrorStack(err))
	} else {
		// log.Info("insert")
	}

	err = conn.Rollback()
	if err != nil {
		log.Fatalf("rollback fatal %+v\n", errors.ErrorStack(err))
	} else {
		// log.Info("rollback")
	}

	err = conn.Commit()
	if err != nil {
		log.Fatalf("commit fatal %+v\n", errors.ErrorStack(err))
	} else {
		// log.Info("commit")
	}

	rows, err := conn.Select("SELECT COUNT(1) FROM t")
	if err != nil {
		log.Fatalf("select fatal %+v\n", errors.ErrorStack(err))
	} else {
		if rows[0][0].ValString != "0" {
			log.Fatalf("expected row count %s got %s\n", "0", rows[0][0].ValString)
		}
	}

	conn.CloseDB()
}
