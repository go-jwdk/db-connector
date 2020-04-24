package pq

import (
	"fmt"
	"strings"

	conn "github.com/go-jwdk/db-connector"
	"github.com/go-jwdk/db-connector/config"
	"github.com/go-jwdk/jobworker"
	"github.com/lib/pq"
)

const name = "postgres"

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
	return conn.Open(&conn.Config{
		Name:                  name,
		DSN:                   cfg.DSN,
		MaxOpenConns:          cfg.MaxOpenConns,
		MaxIdleConns:          cfg.MaxIdleConns,
		ConnMaxLifetime:       cfg.ConnMaxLifetime,
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

type sqlTemplate struct {
}

func (sqlTemplate) NewFindJobDML(table string, jobID string) (string, []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE job_id=?
`
	return fmt.Sprintf(query, conn.TablePrefix, table), []interface{}{jobID}
}

func (sqlTemplate) NewFindJobsDML(table string, limit int64) (stmt string, args []interface{}) {
	query := `
SELECT * FROM %s_%s WHERE invisible_until <= extract(epoch from now()) ORDER BY sec_id DESC LIMIT %d
`
	return fmt.Sprintf(query, conn.TablePrefix, table, limit), []interface{}{}
}

func (sqlTemplate) NewHideJobDML(table string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s
SET retry_count=retry_count+1, invisible_until=extract(epoch from now())+?
WHERE
  job_id=? AND
  retry_count=? AND
  invisible_until=?
`
	return fmt.Sprintf(query, conn.TablePrefix, table), []interface{}{invisibleTime, jobID, oldRetryCount, oldInvisibleUntil}
}

func (sqlTemplate) NewEnqueueJobDML(table, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at)
VALUES (?, ?, ?, ?, 0, extract(epoch from now()) + ?, extract(epoch from now()) ))
`
	return fmt.Sprintf(query, conn.TablePrefix, table), []interface{}{jobID, content, deduplicationID, groupID, delaySeconds}
}

func (sqlTemplate) NewEnqueueJobWithTimeDML(table, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{}) {
	query := `
INSERT INTO %s_%s (job_id, content, deduplication_id, group_id, retry_count, invisible_until, enqueue_at) 
VALUES (?, ?, ?, ?, 0, 0, ?)
`
	return fmt.Sprintf(query, conn.TablePrefix, table), []interface{}{jobID, content, deduplicationID, groupID, enqueueAt}
}

func (sqlTemplate) NewDeleteJobDML(table, jobID string) (stmt string, args []interface{}) {
	query := `
DELETE FROM %s_%s WHERE job_id = ?
`
	return fmt.Sprintf(query, conn.TablePrefix, table),
		[]interface{}{jobID}
}

func (sqlTemplate) NewFindQueueAttributesDML(queueName string) (string, []interface{}) {
	query := `
SELECT * FROM %s_queue_setting WHERE name=?
`
	return fmt.Sprintf(query, conn.TablePrefix),
		[]interface{}{queueName}
}

func (sqlTemplate) NewUpdateJobByVisibilityTimeoutDML(table string, jobID string, visibilityTimeout int64) (stmt string, args []interface{}) {
	query := `
UPDATE %s_%s SET visible_after = extract(epoch from now()) + ? WHERE job_id = ?
`
	return fmt.Sprintf(query, conn.TablePrefix, table), []interface{}{visibilityTimeout, jobID}
}

func (sqlTemplate) NewAddQueueAttributesDML(queueName, queueRawName string, delaySeconds, maxReceiveCount, visibilityTimeout int64, deadLetterTarget *string) (stmt string, args []interface{}) {
	query := `
INSERT INTO %s_queue_setting (name, visibility_timeout, delay_seconds, maximum_message_size, message_retention_period, dead_letter_target, max_receive_count) VALUES (?, ?, ?, ?, ?)
`
	return fmt.Sprintf(query, conn.TablePrefix), []interface{}{queueName, visibilityTimeout, delaySeconds, deadLetterTarget, maxReceiveCount}
}

func (sqlTemplate) NewUpdateQueueAttributesDML(queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (stmt string, args []interface{}) {
	query := `
UPDATE %s_queue_setting SET %s WHERE name = ?
`
	var (
		sets []string
	)
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
		visibility_timeout       INTEGER NOT NULL DEFAULT 30,
		delay_seconds            INTEGER NOT NULL DEFAULT 0,
		dead_letter_target       VARCHAR(255),
		max_receive_count        INTEGER NOT NULL DEFAULT 0,
		UNIQUE(name)
);`
	return fmt.Sprintf(query, conn.TablePrefix)
}

func (sqlTemplate) NewCreateQueueDDL(table string) string {
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
