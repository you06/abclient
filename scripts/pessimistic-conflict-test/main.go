package main

import (
	"fmt"
	"sync"
	"time"
	"math/rand"
	"strconv"
	"github.com/ngaut/log"
	"github.com/you06/doppelganger/connection"
)

const (
	conc = 200
	dsn = "root@tcp(172.16.5.115:4000)/test"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	readyCh := make(chan struct{}, 1)
	for i := 0; i < 1000; i++ {
		corr := rand.Intn(conc) + 10
		target := rand.Intn(corr)
		exec(corr, target)
		go check(target, &readyCh)
		_ = <- readyCh
	}
}

func exec(corr, target int) {
	log.Info("target is", target)
	var wg sync.WaitGroup
	wg.Add(corr)
	for i := 0; i < corr; i++ {
		go func(wg *sync.WaitGroup, i int) {
			conn, err := connection.New(dsn, &connection.Option{
				Mute: true,
			})
			if err != nil {
				log.Error(err)
			}
			conn.Begin()
			time.Sleep(3*time.Second)
			if rand.Float32() < 0.5 {
				if err := conn.Exec(fmt.Sprintf("UPDATE t SET c1 = %d WHERE c2 = 2", i)); err != nil {
					log.Error(err)
				}
			} else {
				if err := conn.Exec(fmt.Sprintf("UPDATE t SET c1 = %d WHERE c3 = 3", i)); err != nil {
					log.Error(err)
				}
			}
			if i == target  {
				if err := conn.Commit(); err != nil {
					log.Info("commit", err)
				}
			} else {
				if err := conn.Rollback(); err != nil {
					log.Info("rollback", err)
				}
			}
			time.Sleep(3*time.Second)
			conn.CloseDB()
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
}

func check(target int, ch *chan struct{}) {
	conn, err := connection.New(dsn, &connection.Option{
		Mute: true,
	})
	log.Info("connection done")
	if err != nil {
		log.Fatal(err)
	}
	conn.Begin()
	(*ch) <- struct{}{}
	time.Sleep(time.Duration(rand.Intn(10) + 1) * time.Minute)
	rows, err := conn.Select("SELECT c1 FROM t")
	if err != nil {
		log.Fatal(err)
	}
	if rows[0][0].ValString != strconv.Itoa(target) {
		log.Fatal(rows[0][0], target, "not consistency")
	}
	conn.CloseDB()
}
