package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-jwdk/db-connector"

	"github.com/go-jwdk/jobworker"
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

	values := db.ConnAttrsToValues(attrs)
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

func Open(s *Setting) (*db.Connector, error) {

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

	return &db.Connector{
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

func (SQLTemplateForMySQL) NewFindJobDML(table string, jobID string) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE job_id=?
`
	return fmt.Sprintf(query, db.TablePrefix, table), []interface{}{jobID}
}

func (SQLTemplateForMySQL) NewFindJobsDML(table string, limit int64) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE invisible_until <= UNIX_TIMESTAMP(NOW()) ORDER BY sec_id DESC LIMIT %d
`
	return fmt.Sprintf(query, db.TablePrefix, table, limit), []interface{}{}
}

func (SQLTemplateForMySQL) NewHideJobDML(table string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s
SET retry_count=retry_count+1, invisible_until=UNIX_TIMESTAMP(NOW())+?
WHERE
  job_id=? AND
  retry_count=? AND
  invisible_until=?
`
	return fmt.Sprintf(query, db.TablePrefix, table), []interface{}{invisibleTime, jobID, oldRetryCount, oldInvisibleUntil}
}

func (SQLTemplateForMySQL) NewEnqueueJobDML(table, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, 0, UNIX_TIMESTAMP(NOW()) + ?, UNIX_TIMESTAMP(NOW()) ))
`
	return fmt.Sprintf(query, db.TablePrefix, table), []interface{}{jobID, content, deduplicationID, groupID}
}

func (SQLTemplateForMySQL) NewEnqueueJobWithTimeDML(table, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at) VALUES (?, ?, ?, ?, ?, 0, 0, ?)
`
	return fmt.Sprintf(query, db.TablePrefix, table), []interface{}{jobID, content, deduplicationID, groupID, enqueueAt}
}

func (SQLTemplateForMySQL) NewDeleteJobDML(table, jobID string) (stmt string, args []interface{}) {
	query := `
DELETE FROM %s_%s WHERE job_id = ?
`
	return fmt.Sprintf(query, db.TablePrefix, table),
		[]interface{}{jobID}
}

func (SQLTemplateForMySQL) NewFindQueueAttributeDML(queue string) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_queue_setting WHERE name=?
`
	return fmt.Sprintf(query, db.TablePrefix),
		[]interface{}{queue}
}

func (SQLTemplateForMySQL) NewUpdateJobByVisibilityTimeoutDML(table string, jobID string, visibilityTimeout int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s SET visible_after = UNIX_TIMESTAMP(NOW()) + ? WHERE job_id = ?
`
	return fmt.Sprintf(query, db.TablePrefix, table), []interface{}{visibilityTimeout, jobID}
}

func (SQLTemplateForMySQL) NewAddQueueAttributeDML(queue, table string, delaySeconds, maximumMessageSize, messageRetentionPeriod int64, deadLetterTarget string, maxReceiveCount, visibilityTimeout int64) (string, []interface{}) {
	query := `
INSERT INTO %s_queue_setting (name, visibility_timeout, delay_seconds, maximum_message_size, message_retention_period, dead_letter_target, max_receive_count) VALUES (?, ?, ?, ?, ?, ?, ?)
`
	return fmt.Sprintf(query, db.TablePrefix), []interface{}{queue, visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod, deadLetterTarget, maxReceiveCount}
}

func (SQLTemplateForMySQL) NewUpdateQueueAttributeDML(visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod *int64, deadLetterTarget *string, maxReceiveCount *int64, queue string) (string, []interface{}) {
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
	args = append(args, queue)
	return fmt.Sprintf(query, db.TablePrefix, strings.Join(sets, ",")), args
}

func (SQLTemplateForMySQL) NewCreateQueueAttributesDDL() string {
	query := `
CREATE TABLE IF NOT EXISTS %s_queue_attributes (
        name                     VARCHAR(255) NOT NULL,
		visibility_timeout       INTEGER UNSIGNED NOT NULL DEFAULT 30,
		delay_seconds            INTEGER UNSIGNED NOT NULL DEFAULT 0,
		maximum_message_size     INTEGER UNSIGNED NOT NULL DEFAULT 0,
		message_retention_period INTEGER UNSIGNED NOT NULL DEFAULT 0,
		dead_letter_target         VARCHAR(255),
		max_receive_count        INTEGER UNSIGNED NOT NULL DEFAULT 0,
		UNIQUE(name)
);`
	return fmt.Sprintf(query, db.TablePrefix)
}

func (SQLTemplateForMySQL) NewCreateQueueDDL(table string) string {
	query := `
CREATE TABLE IF NOT EXISTS %s (
        sec_id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        job_id            VARCHAR(255) NOT NULL,
        content           TEXT,
        deduplication_id  VARCHAR(255),
        group_id          VARCHAR(255),
        invisible_until   BIGINT UNSIGNED NOT NULL,
		retry_count       INTEGER UNSIGNED NOT NULL,
        enqueue_at        BIGINT UNSIGNED,

		PRIMARY KEY (sec_id),
        UNIQUE(deduplication_id),
);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_retry_count ON %s (invisible_until, retry_count);
`
	return fmt.Sprintf(query, table, table, table)
}
