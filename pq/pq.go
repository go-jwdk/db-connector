package pq

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-jwdk/jobworker"
	"github.com/lib/pq"
	"github.com/vvatanabe/goretryer/exponential"
)

var isUniqueViolation = func(err error) bool {
	if err == nil {
		return false
	}
	if pgerr, ok := err.(*pq.Error); ok {
		if pgerr.Code == "23505" {
			return true
		}
	}
	return false
}

var isDeadlockDetected = func(err error) bool {
	if err == nil {
		return false
	}
	if pgerr, ok := err.(*pq.Error); ok {
		if pgerr.Code == "40P01" {
			return true
		}
	}
	return false
}

var provider = Provider{}

const connName = "postgres"

func init() {
	jobworker.Register(connName, provider)
}

type Provider struct {
}

func (Provider) Open(attrs map[string]interface{}) (jobworker.Connector, error) {

	values := db_connector.ConnAttrsToValues(attrs)
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

func Open(s *Setting) (*db_connector.Connector, error) {

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

	return &db_connector.Connector{
		ConnName:           connName,
		DB:                 db,
		SQLTemplate:        SQLTemplateForPostgres{},
		IsUniqueViolation:  isUniqueViolation,
		IsDeadlockDetected: isDeadlockDetected,
		Retryer:            er,
	}, nil
}

type SQLTemplateForPostgres struct {
}

func (SQLTemplateForPostgres) NewFindJobDML(table string, jobID string) (string, []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE job_id=?
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table), []interface{}{jobID}
}

func (SQLTemplateForPostgres) NewFindJobsDML(table string, limit int64) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE invisible_until <= extract(epoch from now()) ORDER BY sec_id DESC LIMIT %d
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table, limit), []interface{}{}
}

func (SQLTemplateForPostgres) NewHideJobDML(table string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s
SET retry_count=retry_count+1, invisible_until=extract(epoch from now())+?
WHERE
  job_id=? AND
  retry_count=? AND
  invisible_until=?
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table), []interface{}{invisibleTime, jobID, oldRetryCount, oldInvisibleUntil}
}

func (SQLTemplateForPostgres) NewEnqueueJobDML(table, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, 0, extract(epoch from now()) + ?, extract(epoch from now()) ))
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table), []interface{}{jobID, content, deduplicationID, groupID, delaySeconds}
}

func (SQLTemplateForPostgres) NewEnqueueJobWithTimeDML(table, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at) 
VALUES (?, ?, ?, ?, 0, 0, ?)
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table), []interface{}{jobID, content, deduplicationID, groupID, enqueueAt}
}

func (SQLTemplateForPostgres) NewDeleteJobDML(table, jobID string) (stmt string, args []interface{}) {
	query := `
DELETE FROM %s_%s WHERE job_id = ?
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table),
		[]interface{}{jobID}
}

func (SQLTemplateForPostgres) NewFindQueueAttributeDML(queueName string) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_queue_setting WHERE name=?
`
	return fmt.Sprintf(query, db_connector.TablePrefix),
		[]interface{}{queueName}
}

func (SQLTemplateForPostgres) NewUpdateJobByVisibilityTimeoutDML(table string, jobID string, visibilityTimeout int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s SET visible_after = extract(epoch from now()) + ? WHERE job_id = ?
`
	return fmt.Sprintf(query, db_connector.TablePrefix, table), []interface{}{visibilityTimeout, jobID}
}

func (SQLTemplateForPostgres) NewAddQueueAttributeDML(queueName, table string, delaySeconds, maximumMessageSize, messageRetentionPeriod int64, deadLetterTarget string, maxReceiveCount, visibilityTimeout int64) (string, []interface{}) {
	query := `
INSERT INTO %s_queue_setting (name, visibility_timeout, delay_seconds, maximum_message_size, message_retention_period, dead_letter_target, max_receive_count) VALUES (?, ?, ?, ?, ?, ?, ?)
`
	return fmt.Sprintf(query, db_connector.TablePrefix), []interface{}{queueName, visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod, deadLetterTarget, maxReceiveCount}
}

func (SQLTemplateForPostgres) NewUpdateQueueAttributeDML(visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod *int64, deadLetterTarget *string, maxReceiveCount *int64, queueName string) (string, []interface{}) {
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
	return fmt.Sprintf(query, db_connector.TablePrefix, strings.Join(sets, ",")), args
}

func (SQLTemplateForPostgres) NewCreateQueueAttributesDDL() string {
	query := `
CREATE TABLE IF NOT EXISTS %s_queue_attributes (
        name                     VARCHAR(255) NOT NULL,
		visibility_timeout       INTEGER NOT NULL DEFAULT 30,
		delay_seconds            INTEGER NOT NULL DEFAULT 0,
		maximum_message_size     INTEGER NOT NULL DEFAULT 0,
		message_retention_period INTEGER NOT NULL DEFAULT 0,
		dead_letter_target       VARCHAR(255),
		max_receive_count        INTEGER NOT NULL DEFAULT 0,
		UNIQUE(name)
);`
	return fmt.Sprintf(query, db_connector.TablePrefix)
}

func (SQLTemplateForPostgres) NewCreateQueueDDL(table string) string {
	query := `
CREATE TABLE IF NOT EXISTS %s (
        sec_id            BIGSERIAL,
        job_id            VARCHAR(255) NOT NULL,
        content           TEXT,
        deduplication_id  VARCHAR(255),
        group_id          VARCHAR(255),
        invisible_until   BIGINT NOT NULL,
		retry_count       INTEGER NOT NULL,
        enqueue_at        BIGINT,

		PRIMARY KEY (sec_id),
        UNIQUE(deduplication_id),
		KEY (invisible_until, class),
		KEY (invisible_until, retry_count),
);
CREATE INDEX IF NOT EXISTS %s_idx_invisible_until_retry_count ON %s (invisible_until, retry_count);
`
	return fmt.Sprintf(query, table, table, table)
}
