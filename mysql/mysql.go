package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-job-worker-development-kit/db-connector/internal"
	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-sql-driver/mysql"
	"github.com/vvatanabe/goretryer/exponential"
)

var isUniqueViolation = func(err error) bool {
	if err == nil {
		return false
	}
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		if mysqlErr.Number == 1062 {
			return true
		}
	}
	return false
}

var isDeadlockDetected = func(err error) bool {
	if err == nil {
		return false
	}
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		if mysqlErr.Number == 1213 {
			return true
		}
	}
	return false
}

var provider = Provider{}

const connName = "mysql"

func init() {
	jobworker.Register(connName, provider)
}

type Provider struct {
}

func (Provider) Open(attrs map[string]interface{}) (jobworker.Connector, error) {

	values := internal.ConnAttrsToValues(attrs)
	values.ApplyDefaultValues()

	var s Setting
	s.DSN = values.DSN
	s.MaxOpenConns = values.MaxOpenConns
	s.MaxIdleConns = values.MaxIdleConns
	s.ConnMaxLifetime = values.ConnMaxLifetime
	s.NumMaxRetries = values.NumMaxRetries

	return Open(&s)
}

type Setting struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime *time.Duration
	NumMaxRetries   *int
}

func Open(s *Setting) (*internal.Connector, error) {

	db, err := sql.Open(connName, s.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(s.MaxOpenConns)
	db.SetMaxIdleConns(s.MaxIdleConns)
	if s.ConnMaxLifetime != nil {
		db.SetConnMaxLifetime(*s.ConnMaxLifetime)
	}

	var er exponential.Retryer
	if s.NumMaxRetries != nil {
		er.NumMaxRetries = *s.NumMaxRetries
	}

	return &internal.Connector{
		ConnName:           connName,
		DB:                 db,
		SQLTemplate:        SQLTemplateForMySQL{},
		IsUniqueViolation:  isUniqueViolation,
		IsDeadlockDetected: isDeadlockDetected,
		Retryer:            er,
	}, nil
}

type SQLTemplateForMySQL struct {
}

func (SQLTemplateForMySQL) NewFindJobDML(queueRawName string, jobID string) (string, []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE job_id=?
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName), []interface{}{jobID}
}

func (SQLTemplateForMySQL) NewFindJobsDML(queueRawName string, limit int64) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE invisible_until <= UNIX_TIMESTAMP(NOW()) ORDER BY sec_id DESC LIMIT %d
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName, limit), []interface{}{}
}

func (SQLTemplateForMySQL) NewHideJobDML(queueRawName string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s
SET retry_count=retry_count+1, invisible_until=UNIX_TIMESTAMP(NOW())+?
WHERE
  job_id=? AND
  retry_count=? AND
  invisible_until=?
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName), []interface{}{invisibleTime, jobID, oldRetryCount, oldInvisibleUntil}
}

func (SQLTemplateForMySQL) NewEnqueueJobDML(queueRawName, jobID, args string, class, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, class, args, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, ?, 0, UNIX_TIMESTAMP(NOW()) + ?, UNIX_TIMESTAMP(NOW()) ))
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName), []interface{}{jobID, class, args, deduplicationID, groupID}
}

func (SQLTemplateForMySQL) NewEnqueueJobWithTimeDML(queueRawName, jobID, args string, class, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, class, args, deduplication_id, group_id, retry_count, invisible_until, enqueue_at) VALUES (?, ?, ?, ?, ?, 0, 0, ?)
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName), []interface{}{jobID, class, args, deduplicationID, groupID, enqueueAt}
}

func (SQLTemplateForMySQL) NewDeleteJobDML(queueRawName, jobID string) (stmt string, args []interface{}) {
	query := `
DELETE FROM %s_%s WHERE job_id = ?
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName),
		[]interface{}{jobID}
}

func (SQLTemplateForMySQL) NewFindQueueAttributeDML(queueName string) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_queue_setting WHERE name=?
`
	return fmt.Sprintf(query, internal.PackageName),
		[]interface{}{queueName}
}

func (SQLTemplateForMySQL) NewUpdateJobByVisibilityTimeoutDML(queueRawName string, jobID string, visibilityTimeout int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s SET visible_after = UNIX_TIMESTAMP(NOW()) + ? WHERE job_id = ?
`
	return fmt.Sprintf(query, internal.PackageName, queueRawName), []interface{}{visibilityTimeout, jobID}
}

func (SQLTemplateForMySQL) NewAddQueueAttributeDML(queueName, queueRawName string, delaySeconds, maximumMessageSize, messageRetentionPeriod int64, deadLetterTarget string, maxReceiveCount, visibilityTimeout int64) (string, []interface{}) {
	query := `
INSERT INTO %s_queue_setting (name, visibility_timeout, delay_seconds, maximum_message_size, message_retention_period, dead_letter_target, max_receive_count) VALUES (?, ?, ?, ?, ?, ?, ?)
`
	return fmt.Sprintf(query, internal.PackageName), []interface{}{queueName, visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod, deadLetterTarget, maxReceiveCount}
}

func (SQLTemplateForMySQL) NewUpdateQueueAttributeDML(visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod *int64, deadLetterTarget *string, maxReceiveCount *int64, queueName string) (string, []interface{}) {
	query := `
UPDATE %s_queue_setting SET %s WHERE name = ?
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
	args = append(args, queueName)
	return fmt.Sprintf(query, internal.PackageName, strings.Join(sets, ",")), args
}

func (SQLTemplateForMySQL) NewCreateQueueAttributeDDL() string {
	query := `
CREATE TABLE IF NOT EXISTS %s_queue_setting (
        name                     VARCHAR(255) NOT NULL,
		visibility_timeout       INTEGER UNSIGNED NOT NULL DEFAULT 30,
		delay_seconds            INTEGER UNSIGNED NOT NULL DEFAULT 0,
		maximum_message_size     INTEGER UNSIGNED NOT NULL DEFAULT 0,
		message_retention_period INTEGER UNSIGNED NOT NULL DEFAULT 0,
		dead_letter_target         VARCHAR(255),
		max_receive_count        INTEGER UNSIGNED NOT NULL DEFAULT 0,
		UNIQUE(name)
);`
	return fmt.Sprintf(query, internal.PackageName)
}

func (SQLTemplateForMySQL) NewCreateQueueDDL(queueRawName string) string {
	query := `
CREATE TABLE IF NOT EXISTS %s (
        sec_id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        job_id            VARCHAR(255) NOT NULL,
        class             VARCHAR(255),
        args              TEXT,
        deduplication_id  VARCHAR(255),
        group_id          VARCHAR(255),
        invisible_until   BIGINT UNSIGNED NOT NULL,
		retry_count       INTEGER UNSIGNED NOT NULL,
        enqueue_at        BIGINT UNSIGNED,

		PRIMARY KEY (sec_id),
        UNIQUE(deduplication_id),
);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_class ON %s (invisible_until, class);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_retry_count ON %s (invisible_until, retry_count);
`
	tablaName := fmt.Sprintf("%s_%s", internal.PackageName, queueRawName)
	return fmt.Sprintf(query, tablaName, tablaName, tablaName, tablaName, tablaName)
}
