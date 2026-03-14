package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yezzey-gp/yproxy/config"

	"golang.org/x/sync/semaphore"
)

func TestCreateSessionHTTPClient(t *testing.T) {
	cnf := &config.Storage{
		StorageEndpoint:         "https://storage.example.com",
		StorageRegion:           "us-east-1",
		S3DialTimeout:           config.Duration(10 * time.Second),
		S3ResponseHeaderTimeout: config.Duration(30 * time.Second),
		S3IdleConnTimeout:       config.Duration(90 * time.Second),
		StorageConcurrency:      1,
		AccessKeyId:             "test_key",
		SecretAccessKey:         "test_secret",
	}
	pool := &S3SessionPool{
		cnf: cnf,
		sem: semaphore.NewWeighted(cnf.StorageConcurrency),
	}
	cr := &config.StorageCredentials{
		AccessKeyId:     "test_key",
		SecretAccessKey: "test_secret",
	}

	sess, err := pool.createSession(cr)
	assert.NoError(t, err)
	assert.NotNil(t, sess)
	assert.NotNil(t, sess.Config.HTTPClient)

	transport, ok := sess.Config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok, "HTTPClient.Transport should be *http.Transport")
	assert.Equal(t, 30*time.Second, transport.ResponseHeaderTimeout)
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout)
	assert.Equal(t, 256, transport.MaxIdleConnsPerHost)
	assert.NotNil(t, transport.DialContext)

	// http.Client.Timeout must NOT be set (would kill streaming operations)
	assert.Equal(t, time.Duration(0), sess.Config.HTTPClient.Timeout)
}

func TestCreateSessionHTTPClientCustomTimeouts(t *testing.T) {
	cnf := &config.Storage{
		StorageEndpoint:         "https://storage.example.com",
		StorageRegion:           "us-east-1",
		S3DialTimeout:           config.Duration(5 * time.Second),
		S3ResponseHeaderTimeout: config.Duration(15 * time.Second),
		S3IdleConnTimeout:       config.Duration(45 * time.Second),
		StorageConcurrency:      1,
		AccessKeyId:             "test_key",
		SecretAccessKey:         "test_secret",
	}
	pool := &S3SessionPool{
		cnf: cnf,
		sem: semaphore.NewWeighted(cnf.StorageConcurrency),
	}
	cr := &config.StorageCredentials{
		AccessKeyId:     "test_key",
		SecretAccessKey: "test_secret",
	}

	sess, err := pool.createSession(cr)
	assert.NoError(t, err)

	transport, ok := sess.Config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, 15*time.Second, transport.ResponseHeaderTimeout)
	assert.Equal(t, 45*time.Second, transport.IdleConnTimeout)
}

func TestContextTimeout(t *testing.T) {
	// Create an HTTP server that never responds (simulates S3 outage)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(30 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	cnf := &config.Storage{
		StorageEndpoint:         srv.URL,
		StorageRegion:           "us-east-1",
		StorageBucket:           "test-bucket",
		StoragePrefix:           "prefix/",
		S3DialTimeout:           config.Duration(10 * time.Second),
		S3ResponseHeaderTimeout: config.Duration(30 * time.Second),
		S3IdleConnTimeout:       config.Duration(90 * time.Second),
		S3OperationTimeout:      config.Duration(60 * time.Second),
		S3StreamTimeout:         config.Duration(2 * time.Second),
		StorageConcurrency:      1,
		AccessKeyId:             "test",
		SecretAccessKey:         "test",
	}

	pool := &S3SessionPool{
		cnf: cnf,
		sem: semaphore.NewWeighted(cnf.StorageConcurrency),
	}
	si := &S3StorageInteractor{
		pool:          pool,
		cnf:           cnf,
		TSToBucketMap: map[string]string{"pg_default": "test-bucket"},
		credentialMap: map[string]config.StorageCredentials{
			"test-bucket": {AccessKeyId: "test", SecretAccessKey: "test"},
		},
	}

	ctx := context.Background()
	start := time.Now()
	_, err := si.CatFileFromStorage(ctx, "test-key", 0, nil)
	elapsed := time.Since(start)

	assert.Error(t, err)
	// Should fail within ~2s (S3StreamTimeout), not hang for 30s
	assert.Less(t, elapsed, 5*time.Second, "operation should timeout within 5s (S3StreamTimeout=2s + overhead)")
}

func TestContextCancellation(t *testing.T) {
	// Create an HTTP server that never responds
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(30 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	cnf := &config.Storage{
		StorageEndpoint:         srv.URL,
		StorageRegion:           "us-east-1",
		StorageBucket:           "test-bucket",
		StoragePrefix:           "prefix/",
		S3DialTimeout:           config.Duration(10 * time.Second),
		S3ResponseHeaderTimeout: config.Duration(30 * time.Second),
		S3IdleConnTimeout:       config.Duration(90 * time.Second),
		S3OperationTimeout:      config.Duration(60 * time.Second),
		S3StreamTimeout:         config.Duration(30 * time.Second),
		StorageConcurrency:      1,
		AccessKeyId:             "test",
		SecretAccessKey:         "test",
	}

	pool := &S3SessionPool{
		cnf: cnf,
		sem: semaphore.NewWeighted(cnf.StorageConcurrency),
	}
	si := &S3StorageInteractor{
		pool:          pool,
		cnf:           cnf,
		TSToBucketMap: map[string]string{"pg_default": "test-bucket"},
		credentialMap: map[string]config.StorageCredentials{
			"test-bucket": {AccessKeyId: "test", SecretAccessKey: "test"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := si.CatFileFromStorage(ctx, "test-key", 0, nil)
		done <- err
	}()

	// Cancel context after 500ms
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		assert.Error(t, err)
		// Should return shortly after cancellation, not wait for full timeout
		assert.Less(t, elapsed, 3*time.Second, "operation should cancel quickly after context cancellation")
	case <-time.After(10 * time.Second):
		t.Fatal("CatFileFromStorage did not return after context cancellation")
	}
}
