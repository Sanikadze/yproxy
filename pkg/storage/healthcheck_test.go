package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yezzey-gp/aws-sdk-go/service/s3"
	"github.com/yezzey-gp/yproxy/config"
)

type mockSessionPool struct {
	sess *s3.S3
	err  error
}

func (m *mockSessionPool) GetSession(ctx context.Context, cr *config.StorageCredentials) (*s3.S3, error) {
	return m.sess, m.err
}

func (m *mockSessionPool) StorageUsedConcurrency() int {
	return 0
}

func TestCheckS3HealthReturnsUnhealthyOnSessionError(t *testing.T) {
	pool := &mockSessionPool{
		sess: nil,
		err:  context.DeadlineExceeded,
	}
	cr := &config.StorageCredentials{}

	healthy, dur := checkS3Health(context.Background(), pool, cr, "test-bucket")
	assert.False(t, healthy)
	assert.Greater(t, dur.Nanoseconds(), int64(0))
}

func TestBoolToFloat(t *testing.T) {
	assert.Equal(t, float64(1), boolToFloat(true))
	assert.Equal(t, float64(0), boolToFloat(false))
}

func TestRunHealthCheckRespectsContextCancellation(t *testing.T) {
	pool := &mockSessionPool{
		sess: nil,
		err:  context.DeadlineExceeded,
	}
	cnf := &config.Storage{
		StorageBucket: "test-bucket",
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunHealthCheck(ctx, pool, cnf, 100*time.Millisecond)
		close(done)
	}()

	// Let it run a couple ticks
	time.Sleep(250 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// goroutine exited as expected
	case <-time.After(2 * time.Second):
		t.Fatal("RunHealthCheck did not exit after context cancellation")
	}
}
