package sqlite3

import (
	"database/sql"
	"fmt"
	"strings"

	conn "github.com/go-jwdk/db-connector"
	"github.com/go-jwdk/db-connector/config"
	"github.com/go-jwdk/jobworker"
	"github.com/mattn/go-sqlite3"
)

const name = "sqlite3"

var (
	provider = Provider{}
	template = sqlTemplate{}
)

func init() {
	jobworker.Register(name, provider)
}

type Provider struct{}

func (Provider) Open(cfgMap map[string]interface{}) (jobworker.Connector, error) {
	cfg := config.ParseConfig(cfgMap)
	return Open(cfg)
}

func Open(cfg *config.Config) (*conn.Connector, error) {
	cfg.ApplyDefaultValues()

	db, err := sql.Open(name, cfg.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	if cfg.ConnMaxLifetime != nil {
		db.SetConnMaxLifetime(*cfg.ConnMaxLifetime)
	}

	return conn.Open(&conn.Config{
		Name:                  name,
		DB:                    db,
		NumMaxRetries:         *cfg.NumMaxRetries,
		QueueAttributesExpire: *cfg.QueueAttributesExpire,
		SQLTemplate:           template,
		IsUniqueViolation:     isUniqueViolation,
		IsDeadlockDetected:    isDeadlockDetected,
	})
}

var isUniqueViolation = func(err error) bool {
	if err == nil {
		return false
	}
	sqliteErr, ok := err.(sqlite3.Error)
	if ok &&
		sqliteErr.Code == sqlite3.ErrConstraint &&
		sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}
	return false
}

var isDeadlockDetected = func(err error) bool {
	sqliteErr, ok := err.(sqlite3.Error)
	if ok &&
		sqliteErr.Code == sqlite3.ErrBusy &&
		sqliteErr.Code != sqlite3.ErrLocked {
		return true
	}
	return false
}

type sqlTemplate struct {
}

func (sqlTemplate) NewFindJobDML(table string, jobID string) (string, []interface{}) {
	query := `
SELECT * FROM %s WHERE job_id=?
`
	return fmt.Sprintf(query, table), []interface{}{jobID}
}

func (sqlTemplate) NewFindJobsDML(table string, limit int64) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s WHERE invisible_until <= strftime('%%s', 'now') ORDER BY sec_id ASC LIMIT %d
`
	return fmt.Sprintf(query, table, limit), []interface{}{}
}

func (sqlTemplate) NewHideJobDML(table string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s
SET retry_count=retry_count+1, invisible_until=strftime('%%s', 'now')+?
WHERE job_id=? AND retry_count=? AND invisible_until=?
`
	return fmt.Sprintf(query, table), []interface{}{invisibleTime, jobID, oldRetryCount, oldInvisibleUntil}
}

func (sqlTemplate) NewEnqueueJobDML(table, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) (stmt string, args []interface{}) {
	query := `
INSERT INTO %s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, 0, strftime('%%s', 'now') + ?, strftime('%%s', 'now') )
`
	return fmt.Sprintf(query, table), []interface{}{jobID, content, deduplicationID, groupID, delaySeconds}

}

func (sqlTemplate) NewEnqueueJobWithTimeDML(table, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) (stmt string, args []interface{}) {
	query := `
INSERT INTO %s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, 0, 0, ?)
`
	return fmt.Sprintf(query, table), []interface{}{jobID, content, deduplicationID, groupID, enqueueAt}
}

func (sqlTemplate) NewDeleteJobDML(table, jobID string) (stmt string, args []interface{}) {
	query := `
DELETE FROM %s WHERE job_id = ?
`
	return fmt.Sprintf(query, table),
		[]interface{}{jobID}
}

func (sqlTemplate) NewFindQueueAttributesDML(queueName string) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_queue_attributes WHERE name=?
`
	return fmt.Sprintf(query, conn.TablePrefix), []interface{}{queueName}
}

func (sqlTemplate) NewUpdateJobByVisibilityTimeoutDML(queueRawName string, jobID string, visibilityTimeout int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s SET invisible_until = strftime('%%s', 'now') + ? WHERE job_id = ?
`
	return fmt.Sprintf(query, queueRawName), []interface{}{visibilityTimeout, jobID}
}

func (sqlTemplate) NewAddQueueAttributesDML(queueName, queueRawName string, delaySeconds, maxReceiveCount, visibilityTimeout int64, deadLetterTarget *string) (stmt string, args []interface{}) {
	query := `
INSERT INTO %s_queue_attributes (name, raw_name, visibility_timeout, delay_seconds, dead_letter_target, max_receive_count)
VALUES (?, ?, ?, ?, ?, ?)
`
	return fmt.Sprintf(query, conn.TablePrefix), []interface{}{queueName, queueRawName, visibilityTimeout, delaySeconds, deadLetterTarget, maxReceiveCount}
}

func (sqlTemplate) NewUpdateQueueAttributesDML(queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (stmt string, args []interface{}) {
	query := `
UPDATE %s_queue_attributes SET %s WHERE raw_name = ?
`
	var sets []string
	if visibilityTimeout != nil {
		sets = append(sets, "visibility_timeout=?")
		args = append(args, *visibilityTimeout)
	}
	if delaySeconds != nil {
		sets = append(sets, "delay_seconds=?")
		args = append(args, *delaySeconds)
	}
	if deadLetterTarget != nil {
		sets = append(sets, "dead_letter_target=?")
		args = append(args, *deadLetterTarget)
	}
	if maxReceiveCount != nil {
		sets = append(sets, "max_receive_count=?")
		args = append(args, *maxReceiveCount)
	}
	args = append(args, queueRawName)
	return fmt.Sprintf(query, conn.TablePrefix, strings.Join(sets, ",")), args
}

func (sqlTemplate) NewCreateQueueAttributesDDL() string {
	query := `
CREATE TABLE IF NOT EXISTS %s_queue_attributes (
        name                     VARCHAR(255) NOT NULL,
        raw_name                 VARCHAR(255) NOT NULL,
		visibility_timeout       INTEGER UNSIGNED NOT NULL DEFAULT 30,
		delay_seconds            INTEGER UNSIGNED NOT NULL,
		max_receive_count        INTEGER UNSIGNED NOT NULL,
		dead_letter_target       VARCHAR(255),
		UNIQUE(name)
		UNIQUE(raw_name)
);`
	return fmt.Sprintf(query, conn.TablePrefix)
}

func (sqlTemplate) NewCreateQueueDDL(table string) string {
	query := `
CREATE TABLE IF NOT EXISTS %s (
        sec_id            INTEGER PRIMARY KEY,
        job_id            VARCHAR(255) NOT NULL,
        content           TEXT,
        deduplication_id  VARCHAR(255),
        group_id          VARCHAR(255),
        invisible_until   INTEGER UNSIGNED NOT NULL,
		retry_count       INTEGER UNSIGNED NOT NULL,
        enqueue_at        INTEGER UNSIGNED NOT NULL,
        UNIQUE(deduplication_id)
);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_retry_count ON %s (invisible_until, retry_count);
`
	return fmt.Sprintf(query, table, table, table)
}
