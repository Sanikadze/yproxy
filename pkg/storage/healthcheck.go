package storage

import (
	"context"
	"time"

	"github.com/yezzey-gp/aws-sdk-go/aws"
	"github.com/yezzey-gp/aws-sdk-go/service/s3"
	"github.com/yezzey-gp/yproxy/config"
	"github.com/yezzey-gp/yproxy/pkg/metrics"
	"github.com/yezzey-gp/yproxy/pkg/ylogger"
)

// RunHealthCheck periodically probes S3 availability via HeadBucket
// and updates Prometheus gauges. It does NOT block new operations when
// S3 is unhealthy — that is a separate concern.
func RunHealthCheck(ctx context.Context, pool SessionPool, cnf *config.Storage, interval time.Duration) {
	bucket := cnf.StorageBucket
	cr := config.StorageCredentials{
		AccessKeyId:     cnf.AccessKeyId,
		SecretAccessKey: cnf.SecretAccessKey,
	}

	wasHealthy := true // assume healthy at start

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			healthy, dur := checkS3Health(ctx, pool, &cr, bucket)
			metrics.S3Healthy.Set(boolToFloat(healthy))
			metrics.S3LastCheckDuration.Set(dur.Seconds())

			if healthy && !wasHealthy {
				ylogger.Zero.Info().Str("bucket", bucket).Dur("latency", dur).Msg("S3 endpoint recovered")
			} else if !healthy && wasHealthy {
				ylogger.Zero.Error().Str("bucket", bucket).Dur("latency", dur).Msg("S3 endpoint unreachable")
			}
			wasHealthy = healthy
		}
	}
}

func checkS3Health(ctx context.Context, pool SessionPool, cr *config.StorageCredentials, bucket string) (bool, time.Duration) {
	start := time.Now()

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	sess, err := pool.GetSession(checkCtx, cr)
	if err != nil {
		return false, time.Since(start)
	}

	_, err = sess.HeadBucketWithContext(checkCtx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	return err == nil, time.Since(start)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
