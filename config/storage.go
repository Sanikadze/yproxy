package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

type Crypto struct {
	GPGKeyId   string `json:"gpg_key_id" toml:"gpg_key_id" yaml:"gpg_key_id"`
	GPGKeyPath string `json:"gpg_key_path" toml:"gpg_key_path" yaml:"gpg_key_path"`
}

type StorageCredentials struct {
	AccessKeyId     string `json:"access_key_id" toml:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key" toml:"secret_access_key" yaml:"secret_access_key"`
}

type Storage struct {
	StorageEndpoint string `json:"storage_endpoint" toml:"storage_endpoint" yaml:"storage_endpoint"`
	AccessKeyId     string `json:"access_key_id" toml:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key" toml:"secret_access_key" yaml:"secret_access_key"`
	StoragePrefix   string `json:"storage_prefix" toml:"storage_prefix" yaml:"storage_prefix"`
	StorageBucket   string `json:"storage_bucket" toml:"storage_bucket" yaml:"storage_bucket"`

	CredentialMap map[string]StorageCredentials `json:"credential_map" toml:"credential_map" yaml:"credential_map"`

	TablespaceMap map[string]string `json:"tablespace_map" toml:"tablespace_map" yaml:"tablespace_map"`

	// how many concurrent connection acquire allowed
	StorageConcurrency int64 `json:"storage_concurrency" toml:"storage_concurrency" yaml:"storage_concurrency"`

	// default will be false
	EnableRateLimiter bool   `json:"enable_rate_limiter" toml:"enable_rate_limiter" yaml:"enable_rate_limiter"`
	StorageRateLimit  uint64 `json:"storage_rate_limit" toml:"storage_rate_limit" yaml:"storage_rate_limit"`

	StorageRegion string `json:"storage_region" toml:"storage_region" yaml:"storage_region"`

	// File storage default s3. Available: s3, fs
	StorageType string `json:"storage_type" toml:"storage_type" yaml:"storage_type"`

	EndpointSourceHost   string `json:"storage_endpoint_source_host" toml:"storage_endpoint_source_host" yaml:"storage_endpoint_source_host"`
	EndpointSourcePort   string `json:"storage_endpoint_source_port" toml:"storage_endpoint_source_port" yaml:"storage_endpoint_source_port"`
	EndpointSourceScheme string `json:"storage_endpoint_source_scheme" toml:"storage_endpoint_source_scheme" yaml:"storage_endpoint_source_scheme"`

	// S3 HTTP transport timeouts
	S3DialTimeout           Duration `json:"s3_dial_timeout" toml:"s3_dial_timeout" yaml:"s3_dial_timeout"`
	S3ResponseHeaderTimeout Duration `json:"s3_response_header_timeout" toml:"s3_response_header_timeout" yaml:"s3_response_header_timeout"`
	S3IdleConnTimeout       Duration `json:"s3_idle_conn_timeout" toml:"s3_idle_conn_timeout" yaml:"s3_idle_conn_timeout"`

	// S3 operation-level timeouts (context deadlines)
	S3OperationTimeout Duration `json:"s3_operation_timeout" toml:"s3_operation_timeout" yaml:"s3_operation_timeout"` // metadata ops: list, delete, head, copy
	S3StreamTimeout    Duration `json:"s3_stream_timeout" toml:"s3_stream_timeout" yaml:"s3_stream_timeout"`          // streaming ops: get, put
}

func (s Storage) ID() string {
	id, _ := url.JoinPath(s.StorageEndpoint, s.StorageBucket, s.StoragePrefix)
	return id
}

// Duration wraps time.Duration to support human-readable strings (e.g. "10s", "5m")
// in YAML and JSON config files. gopkg.in/yaml.v2 cannot natively unmarshal strings
// into time.Duration, so this custom type handles both string and numeric formats.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err == nil {
		td, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(td)
		return nil
	}
	var n int64
	if err := unmarshal(&n); err != nil {
		return fmt.Errorf("cannot unmarshal duration: expected string or number")
	}
	*d = Duration(n)
	return nil
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		*d = Duration(n)
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("cannot unmarshal duration: expected string or number")
	}
	td, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(td)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalText(text []byte) error {
	td, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(td)
	return nil
}
