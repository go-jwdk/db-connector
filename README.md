# db-connector

A jobworker connector with database for [go-jwdk/jobworker](https://github.com/go-jwdk/jobworker) package.

Supported databases:

- MySQL
- Postgres
- SQLite3

## Requirements

Go 1.13+

## Installation

This package can be installed with the go get command:

```
$ go get -u github.com/go-jwdk/db-connector
```

## Usage

__MySQL__:

```go
import "github.com/go-jwdk/jobworker"
import _ "github.com/go-jwdk/db-connector/mysql"

conn, err := jobworker.Open("mysql", map[string]interface{}{
    "DSN":             "user:password@/dbname",
    "MaxOpenConns":    3,
    "MaxMaxIdleConns": 3,
    "ConnMaxLifetime": time.Minute,
    "NumMaxRetries":   3,
})
```

__Postgres__:

```go
import "github.com/go-jwdk/jobworker"
import _ "github.com/go-jwdk/db-connector/postgres"

conn, err := jobworker.Open("postgres", map[string]interface{}{
    "DSN":             "user=pqgotest dbname=pqgotest sslmode=verify-full",
    "MaxOpenConns":    3,
    "MaxMaxIdleConns": 3,
    "ConnMaxLifetime": time.Minute,
    "NumMaxRetries":   3,
})
```

__SQLite3__:

```go
import "github.com/go-jwdk/jobworker"
import _ "github.com/go-jwdk/db-connector/sqlite3"

conn, err := jobworker.Open("sqlite3", map[string]interface{}{
    "DSN":             "file:test.db?cache=shared&mode=memory",
    "MaxOpenConns":    3,
    "MaxMaxIdleConns": 3,
    "ConnMaxLifetime": time.Minute,
    "NumMaxRetries":   3,
})
```

