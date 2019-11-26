package main

import (
	"sync"
	"time"
	"github.com/ngaut/log"
	"github.com/you06/doppelganger/connection"
)

const (
	dsn = "root@tcp(172.17.0.1:4000)/repro"
)

func main() {
	for i := 0; i < 1000; i++ {
		once()
		log.Info(i, "done.")
	}
}

func once() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		conn, err := connection.New(dsn, &connection.Option{
			Mute: true,
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := conn.Begin(); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second)
		if err := conn.Exec("UPDATE t SET d = 3 WHERE c = 1;"); err != nil {
			log.Fatal(err)
		}
		time.Sleep(3*time.Second)
		if err := conn.Commit(); err != nil {
			log.Fatal(err)
		}
		wg.Done()
	}()
	go func() {
		conn, err := connection.New(dsn, &connection.Option{
			Mute: true,
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := conn.Begin(); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second)
		if err := conn.Exec("UPDATE t SET d = 4 WHERE c = 1;"); err != nil {
			log.Fatal(err)
		}
		time.Sleep(3*time.Second)
		if err := conn.Commit(); err != nil {
			log.Fatal(err)
		}
		wg.Done()
	}()
	wg.Wait()
}
