package config

import "time"

const (
	ConnectorAttributeNameDSN                   = "DSN"
	ConnectorAttributeNameMaxOpenConns          = "MaxOpenConns"
	ConnectorAttributeNameMaxIdleConns          = "MaxMaxIdleConns"
	ConnectorAttributeNameConnMaxLifetime       = "ConnMaxLifetime"
	ConnectorAttributeNameNumMaxRetries         = "NumMaxRetries"
	ConnectorAttributeNameQueueAttributesExpire = "QueueAttributesExpire"

	DefaultNumMaxRetries         = 3
	DefaultQueueAttributesExpire = time.Minute
)

type Config struct {
	DSN                   string
	MaxOpenConns          int
	MaxIdleConns          int
	ConnMaxLifetime       *time.Duration
	NumMaxRetries         *int
	QueueAttributesExpire *time.Duration
}

func (v *Config) ApplyDefaultValues() {
	if v.NumMaxRetries == nil {
		i := DefaultNumMaxRetries
		v.NumMaxRetries = &i
	}
	if v.QueueAttributesExpire == nil {
		i := DefaultQueueAttributesExpire
		v.QueueAttributesExpire = &i
	}
}

func ParseConfig(cfgMap map[string]interface{}) *Config {
	var cfg Config
	for k, v := range cfgMap {
		switch k {
		case ConnectorAttributeNameDSN:
			s := v.(string)
			cfg.DSN = s
		case ConnectorAttributeNameMaxOpenConns:
			s := v.(int)
			cfg.MaxOpenConns = s
		case ConnectorAttributeNameMaxIdleConns:
			s := v.(int)
			cfg.MaxIdleConns = s
		case ConnectorAttributeNameConnMaxLifetime:
			s := v.(time.Duration)
			cfg.ConnMaxLifetime = &s
		case ConnectorAttributeNameNumMaxRetries:
			s := v.(int)
			cfg.NumMaxRetries = &s
		case ConnectorAttributeNameQueueAttributesExpire:
			s := v.(time.Duration)
			cfg.QueueAttributesExpire = &s
		}
	}
	return &cfg
}
