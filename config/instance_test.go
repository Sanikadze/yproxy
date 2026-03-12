package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmbedDefaults_S3Timeouts(t *testing.T) {
	cfg := &Instance{}
	EmbedDefaults(cfg)

	assert.Equal(t, 10*time.Second, cfg.StorageCnf.S3DialTimeout)
	assert.Equal(t, 30*time.Second, cfg.StorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, 90*time.Second, cfg.StorageCnf.S3IdleConnTimeout)
	assert.Equal(t, 60*time.Second, cfg.StorageCnf.S3OperationTimeout)
	assert.Equal(t, 5*time.Minute, cfg.StorageCnf.S3StreamTimeout)

	assert.Equal(t, 10*time.Second, cfg.BackupStorageCnf.S3DialTimeout)
	assert.Equal(t, 30*time.Second, cfg.BackupStorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, 90*time.Second, cfg.BackupStorageCnf.S3IdleConnTimeout)
	assert.Equal(t, 60*time.Second, cfg.BackupStorageCnf.S3OperationTimeout)
	assert.Equal(t, 5*time.Minute, cfg.BackupStorageCnf.S3StreamTimeout)
}

func TestEmbedDefaults_S3Timeouts_CustomValues(t *testing.T) {
	cfg := &Instance{
		StorageCnf: Storage{
			S3DialTimeout:           5 * time.Second,
			S3ResponseHeaderTimeout: 15 * time.Second,
			S3IdleConnTimeout:       60 * time.Second,
		},
	}
	EmbedDefaults(cfg)

	// Custom values should be preserved
	assert.Equal(t, 5*time.Second, cfg.StorageCnf.S3DialTimeout)
	assert.Equal(t, 15*time.Second, cfg.StorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, 60*time.Second, cfg.StorageCnf.S3IdleConnTimeout)

	// Backup should get defaults since not set
	assert.Equal(t, 10*time.Second, cfg.BackupStorageCnf.S3DialTimeout)
	assert.Equal(t, 30*time.Second, cfg.BackupStorageCnf.S3ResponseHeaderTimeout)
	assert.Equal(t, 90*time.Second, cfg.BackupStorageCnf.S3IdleConnTimeout)
}
