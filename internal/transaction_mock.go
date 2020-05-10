// +build mock

package internal

import (
	"context"
	"database/sql"
)

type querierMock struct {
}

func (q querierMock) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

func (q querierMock) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (q querierMock) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}

func WithTransaction(db *sql.DB, ope func(tx Querier) error) (err error) {
	return ope(querierMock{})
}
