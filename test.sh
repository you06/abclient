#!/bin/bash

rm -rf test-log/log
mkdir test-log/log
./bin/doppelganger -dsn1 "root:@tcp(172.17.0.1:33306)/sqlsmith" -dsn2 "root@tcp(172.17.0.1:4000)/sqlsmith" -log ./test-log/log -stable -concurrency 3
