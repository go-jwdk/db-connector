package sqlite3

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/go-job-worker-development-kit/db-connector/internal"
	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/mattn/go-sqlite3"
	"github.com/vvatanabe/goretryer/exponential"
)

var isUniqueViolation = func(err error) bool {

	mysql.ParseDSN()

	if err == nil {
		return false
	}
	sqliteErr, ok := err.(sqlite3.Error)
	if ok && sqliteErr.Code == sqlite3.ErrConstraint && sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}
	return false
}

var isDeadlockDetected = func(err error) bool {
	sqliteErr, ok := err.(sqlite3.Error)
	if ok && sqliteErr.Code == sqlite3.ErrBusy && sqliteErr.Code != sqlite3.ErrLocked {
		return true
	}
	return false
}

var provider = Provider{}

const connName = "sqlite3"

func init() {

	jobworker.Register(connName, provider)
}

type Provider struct {
}

func (Provider) Open(attrs map[string]interface{}) (jobworker.Connector, error) {

	values := internal.ConnAttrsToValues(attrs)
	values.ApplyDefaultValues()

	db, err := sql.Open(connName, values.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(values.MaxOpenConns)
	db.SetMaxIdleConns(values.MaxIdleConns)
	if values.ConnMaxLifetime != nil {
		db.SetConnMaxLifetime(*values.ConnMaxLifetime)
	}

	var er exponential.Retryer
	if values.NumMaxRetries != nil {
		er.NumMaxRetries = *values.NumMaxRetries
	}

	return &internal.Connector{
		Name:               connName,
		DB:                 db,
		SQLTemplate:        SQLTemplateForSQLite3{},
		IsUniqueViolation:  isUniqueViolation,
		IsDeadlockDetected: isDeadlockDetected,
		Retryer:            er,
	}, nil
}

type SQLTemplateForSQLite3 struct {
}

func (SQLTemplateForSQLite3) NewFindJobDML(queueRawName string, jobID string) (string, []interface{}) {
	query := `
SELECT * FROM %s WHERE job_id=?
`
	return fmt.Sprintf(query, queueRawName), []interface{}{jobID}
}

func (SQLTemplateForSQLite3) NewFindJobsDML(queueRawName string, limit int64) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s WHERE invisible_until <= strftime('%%s', 'now') ORDER BY sec_id ASC LIMIT %d
`
	return fmt.Sprintf(query, queueRawName, limit), []interface{}{}
}

func (SQLTemplateForSQLite3) NewHideJobDML(queueRawName string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s
SET retry_count=retry_count+1, invisible_until=strftime('%%s', 'now')+?
WHERE
  job_id=? AND
  retry_count=? AND
  invisible_until=?
`
	return fmt.Sprintf(query, queueRawName), []interface{}{invisibleTime, jobID, oldRetryCount, oldInvisibleUntil}
}

func (SQLTemplateForSQLite3) NewEnqueueJobDML(queueRawName, jobID, class, args string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{}) {
	query := `
INSERT INTO %s (job_id, class, args, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, ?, 0, strftime('%%s', 'now') + ?, strftime('%%s', 'now') )
`
	return fmt.Sprintf(query, queueRawName), []interface{}{jobID, class, args, deduplicationID, groupID, delaySeconds}

}

func (SQLTemplateForSQLite3) NewEnqueueJobWithTimeDML(queueRawName, jobID, class, args string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{}) {
	query := `
INSERT INTO %s (job_id, class, args, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, ?, 0, 0, ?)
`
	return fmt.Sprintf(query, queueRawName), []interface{}{jobID, class, args, deduplicationID, groupID, enqueueAt}
}

func (SQLTemplateForSQLite3) NewDeleteJobDML(queueRawName, jobID string) (stmt string, args []interface{}) {
	query := `
DELETE FROM %s WHERE job_id = ?
`
	return fmt.Sprintf(query, queueRawName),
		[]interface{}{jobID}
}

func (SQLTemplateForSQLite3) NewFindQueueAttributeDML(queueName string) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_queue_attribute WHERE name=?
`
	return fmt.Sprintf(query, internal.PackageName),
		[]interface{}{queueName}
}

func (SQLTemplateForSQLite3) NewUpdateJobByVisibilityTimeoutDML(queueRawName string, jobID string, visibilityTimeout int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s SET visible_after = strftime('%%s', 'now') + ? WHERE job_id = ?
`
	return fmt.Sprintf(query, queueRawName), []interface{}{visibilityTimeout, jobID}
}

func (SQLTemplateForSQLite3) NewAddQueueAttributeDML(queueName, queueRawName string, delaySeconds, maximumMessageSize, messageRetentionPeriod int64, deadLetterTarget string, maxReceiveCount, visibilityTimeout int64) (string, []interface{}) {
	query := `
INSERT INTO %s_queue_attribute (name, raw_name, visibility_timeout, delay_seconds, maximum_message_size, message_retention_period, dead_letter_target, max_receive_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`
	return fmt.Sprintf(query, internal.PackageName), []interface{}{queueName, queueRawName, visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod, deadLetterTarget, maxReceiveCount}
}

func (SQLTemplateForSQLite3) NewUpdateQueueAttributeDML(visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod *int64, deadLetterTarget *string, maxReceiveCount *int64, queueRawName string) (string, []interface{}) {
	query := `
UPDATE %s_queue_attribute SET %s WHERE raw_name = ?
`
	var (
		sets []string
		args []interface{}
	)
	if visibilityTimeout != nil {
		sets = append(sets, "visibility_timeout=?")
		args = append(args, *visibilityTimeout)
	}
	if delaySeconds != nil {
		sets = append(sets, "delay_seconds=?")
		args = append(args, *delaySeconds)
	}
	if maximumMessageSize != nil {
		sets = append(sets, "maximum_message_size=?")
		args = append(args, *maximumMessageSize)
	}
	if messageRetentionPeriod != nil {
		sets = append(sets, "message_retention_period=?")
		args = append(args, *messageRetentionPeriod)
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
	return fmt.Sprintf(query, internal.PackageName, strings.Join(sets, ",")), args
}

func (SQLTemplateForSQLite3) NewCreateQueueAttributeDDL() string {
	query := `
CREATE TABLE IF NOT EXISTS %s_queue_attribute (
        name                     VARCHAR(255) NOT NULL,
        raw_name                 VARCHAR(255) NOT NULL,
		visibility_timeout       INTEGER UNSIGNED NOT NULL DEFAULT 30,
		delay_seconds            INTEGER UNSIGNED NOT NULL DEFAULT 0,
		maximum_message_size     INTEGER UNSIGNED NOT NULL DEFAULT 1,
		message_retention_period INTEGER UNSIGNED NOT NULL DEFAULT 0,
		dead_letter_target       VARCHAR(255),
		max_receive_count        INTEGER UNSIGNED NOT NULL DEFAULT 0,
		UNIQUE(name)
		UNIQUE(raw_name)
);`
	return fmt.Sprintf(query, internal.PackageName)
}

func (SQLTemplateForSQLite3) NewCreateQueueDDL(queueRawName string) string {
	query := `
CREATE TABLE IF NOT EXISTS %s (
        sec_id            INTEGER PRIMARY KEY,
        job_id            VARCHAR(255) NOT NULL,
        class             VARCHAR(255) NOT NULL, 
        args              TEXT,
        deduplication_id  VARCHAR(255),
        group_id          VARCHAR(255),
        invisible_until   INTEGER UNSIGNED NOT NULL,
		retry_count       INTEGER UNSIGNED NOT NULL,
        enqueue_at        INTEGER UNSIGNED,
        UNIQUE(deduplication_id)
);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_class ON %s (invisible_until, class);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_retry_count ON %s (invisible_until, retry_count);
`
	return fmt.Sprintf(query, queueRawName, queueRawName, queueRawName, queueRawName, queueRawName)
}
