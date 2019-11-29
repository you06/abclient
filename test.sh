#!/bin/bash

rm -rf test-log/log
mkdir test-log/log
./bin/doppelganger -concurrency 6 -dsn1 "root:@tcp(172.17.0.1:4000)/sqlsmith" -dsn2 "root@tcp(172.16.5.115:4000)/sqlsmith" -log ./test-log/log -stable
