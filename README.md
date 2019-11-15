# SQLSmith Client

This is the client of [SQLSmith-go](https://github.com/you06/sqlsmith-go).

## Build

```
make
```

## Usage

```
./bin/sqlsmith-client -h
Usage of ./bin/sqlsmith-client:
  -V    print version
  -clear
        drop all tables in target database and then start testing
  -dsn1 string
        dsn1
  -dsn2 string
        dsn2
  -log string
        log path
  -re string
        reproduce from log, path:line, will execute to the line number, will not execute the given line
  -schema
        print schema and exit
```

- ABtest Example

```
./bin/sqlsmith-client -dsn1 "root:@tcp(127.0.0.1:3306)/sqlsmith" -dsn2 "root@tcp(127.0.0.1:4000)/sqlsmith" -log ./log
```

With empty log parameter, all logs will be print to terminal.
