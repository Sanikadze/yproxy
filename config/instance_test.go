package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmbedDefaults_S3Timeouts(t *testing.T) {
	cfg := &Instance{}
	EmbedDefaults(cfg)

	assert.Equal(t, Duration(10*time.Second), cfg.StorageCnf.S3DialTimeout)
	assert.Equal(t, Duration(30*time.Second), cfg.StorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, Duration(90*time.Second), cfg.StorageCnf.S3IdleConnTimeout)
	assert.Equal(t, Duration(60*time.Second), cfg.StorageCnf.S3OperationTimeout)
	assert.Equal(t, Duration(5*time.Minute), cfg.StorageCnf.S3StreamTimeout)

	assert.Equal(t, Duration(10*time.Second), cfg.BackupStorageCnf.S3DialTimeout)
	assert.Equal(t, Duration(30*time.Second), cfg.BackupStorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, Duration(90*time.Second), cfg.BackupStorageCnf.S3IdleConnTimeout)
	assert.Equal(t, Duration(60*time.Second), cfg.BackupStorageCnf.S3OperationTimeout)
	assert.Equal(t, Duration(5*time.Minute), cfg.BackupStorageCnf.S3StreamTimeout)
}

func TestEmbedDefaults_S3Timeouts_CustomValues(t *testing.T) {
	cfg := &Instance{
		StorageCnf: Storage{
			S3DialTimeout:           Duration(5 * time.Second),
			S3ResponseHeaderTimeout: Duration(15 * time.Second),
			S3IdleConnTimeout:       Duration(60 * time.Second),
		},
	}
	EmbedDefaults(cfg)

	// Custom values should be preserved
	assert.Equal(t, Duration(5*time.Second), cfg.StorageCnf.S3DialTimeout)
	assert.Equal(t, Duration(15*time.Second), cfg.StorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, Duration(60*time.Second), cfg.StorageCnf.S3IdleConnTimeout)

	// Backup should get defaults since not set
	assert.Equal(t, Duration(10*time.Second), cfg.BackupStorageCnf.S3DialTimeout)
	assert.Equal(t, Duration(30*time.Second), cfg.BackupStorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, Duration(90*time.Second), cfg.BackupStorageCnf.S3IdleConnTimeout)
}
