package internal

import "database/sql"

func WithTransaction(db *sql.DB, ope func(tx *sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	err = ope(tx)
	return
}

func IsErrNoRows(err error) bool {
	return err == sql.ErrNoRows
}
