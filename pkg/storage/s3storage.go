package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yezzey-gp/aws-sdk-go/aws"
	"github.com/yezzey-gp/aws-sdk-go/service/s3"
	"github.com/yezzey-gp/aws-sdk-go/service/s3/s3manager"
	"github.com/yezzey-gp/yproxy/config"
	"github.com/yezzey-gp/yproxy/pkg/message"
	"github.com/yezzey-gp/yproxy/pkg/metrics"
	"github.com/yezzey-gp/yproxy/pkg/object"
	"github.com/yezzey-gp/yproxy/pkg/settings"
	"github.com/yezzey-gp/yproxy/pkg/tablespace"
	"github.com/yezzey-gp/yproxy/pkg/ylogger"
)

type S3StorageInteractor struct {
	pool SessionPool

	cnf *config.Storage

	TSToBucketMap    map[string]string
	credentialMap    map[string]config.StorageCredentials
	multipartUploads sync.Map
}

// ListBuckets implements StorageInteractor.
func (s *S3StorageInteractor) ListBuckets() []string {
	keys := []string{}

	for _, v := range s.TSToBucketMap {
		keys = append(keys, v)
	}
	return keys
}

// DefaultBucket implements StorageInteractor.
func (s *S3StorageInteractor) DefaultBucket() string {
	return s.cnf.StorageBucket
}

var _ StorageInteractor = &S3StorageInteractor{}

func (s *S3StorageInteractor) CatFileFromStorage(ctx context.Context, name string, offset int64, setts []settings.StorageSettings) (io.ReadCloser, error) {

	timeStart := time.Now()
	objectPath := strings.TrimLeft(path.Join(s.cnf.StoragePrefix, name), "/")
	tableSpace := ResolveStorageSetting(setts, message.TableSpaceSetting, tablespace.DefaultTableSpace)

	bucket, ok := s.TSToBucketMap[tableSpace]
	if !ok {
		err := fmt.Errorf("failed to match tablespace %s to s3 bucket", tableSpace)
		ylogger.Zero.Err(err)
		return nil, err
	}

	// XXX: fix this
	cr := s.credentialMap[bucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		ylogger.Zero.Err(err).Msg("failed to acquire s3 session")
		return nil, err
	}
	input := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String(objectPath),
		Range:  aws.String(fmt.Sprintf("bytes=%d-", offset)),
	}

	ylogger.Zero.Debug().Str("op", "GetObject").Str("bucket", bucket).Str("key", objectPath).Int64("offset", offset).Msg("s3 operation started")

	opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3StreamTimeout))
	object, err := sess.GetObjectWithContext(opCtx, input)
	if err != nil {
		cancel()
		logContextError(err, "GetObject", bucket, objectPath, time.Duration(s.cnf.S3StreamTimeout))
		ylogger.Zero.Error().Str("op", "GetObject").Str("bucket", bucket).Str("key", objectPath).Dur("duration", time.Since(timeStart)).Err(err).Msg("s3 operation failed")
		return nil, err
	}
	getTime := time.Since(timeStart).Nanoseconds()
	objLen := 1.0
	if object.ContentLength != nil {
		objLen = float64(*object.ContentLength)
	}
	metrics.StoreLatencyAndSizeInfo("S3_GET", objLen, float64(getTime))
	ylogger.Zero.Debug().Str("op", "GetObject").Str("bucket", bucket).Str("key", objectPath).Dur("duration", time.Since(timeStart)).Msg("s3 operation completed")
	return &contextCancelReader{ReadCloser: object.Body, cancel: cancel}, nil
}

// contextCancelReader wraps an io.ReadCloser and cancels a context on Close.
type contextCancelReader struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *contextCancelReader) Close() error {
	err := r.ReadCloser.Close()
	r.cancel()
	return err
}

func (s *S3StorageInteractor) PutFileToDest(ctx context.Context, name string, r io.Reader, settings []settings.StorageSettings) error {

	timeStart := time.Now()
	objectPath := strings.TrimLeft(path.Join(s.cnf.StoragePrefix, name), "/")

	storageClass := ResolveStorageSetting(settings, message.StorageClassSetting, "STANDARD")
	tableSpace := ResolveStorageSetting(settings, message.TableSpaceSetting, tablespace.DefaultTableSpace)
	multipartChunkSizeStr := ResolveStorageSetting(settings, message.MultipartChunkSize, "16777216")
	multipartChunkSize, err := strconv.ParseInt(multipartChunkSizeStr, 10, 64)
	if err != nil {
		return err
	}
	multipartUpload, err := strconv.ParseBool(ResolveStorageSetting(settings, message.MultipartUpload, "1"))
	if err != nil {
		return err
	}

	bucket, ok := s.TSToBucketMap[tableSpace]
	if !ok {
		err := fmt.Errorf("failed to match tablespace %s to s3 bucket", tableSpace)
		ylogger.Zero.Err(err)
		return err
	}

	cr := s.credentialMap[bucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		ylogger.Zero.Err(err).Msg("failed to acquire s3 session")
		return err
	}

	ylogger.Zero.Debug().Str("op", "PutObject").Str("bucket", bucket).Str("key", objectPath).Bool("multipart", multipartUpload).Int64("chunk_size", multipartChunkSize).Msg("s3 operation started")

	up := s3manager.NewUploaderWithClient(sess, func(uploader *s3manager.Uploader) {
		uploader.PartSize = int64(multipartChunkSize)
		uploader.Concurrency = 1
	})
	putLen := int(multipartChunkSize)
	if multipartUpload {
		s.multipartUploads.Store(objectPath, true)
		opCtx, opCancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3StreamTimeout))
		defer opCancel()
		_, err = up.UploadWithContext(opCtx,
			&s3manager.UploadInput{
				Bucket:       aws.String(bucket),
				Key:          aws.String(objectPath),
				Body:         r,
				StorageClass: aws.String(storageClass),
			},
		)
		s.multipartUploads.Delete(objectPath)
	} else {
		var body []byte
		body, err = io.ReadAll(r)
		if err != nil {
			return err
		}
		putLen = len(body)
		opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3StreamTimeout))
		defer cancel()
		_, err = sess.PutObjectWithContext(opCtx, &s3.PutObjectInput{
			Bucket:       aws.String(bucket),
			Key:          aws.String(objectPath),
			Body:         bytes.NewReader(body),
			StorageClass: aws.String(storageClass),
		})
	}
	dur := time.Since(timeStart)
	if err != nil {
		logContextError(err, "PutObject", bucket, objectPath, time.Duration(s.cnf.S3StreamTimeout))
		ylogger.Zero.Error().Str("op", "PutObject").Str("bucket", bucket).Str("key", objectPath).Dur("duration", dur).Err(err).Msg("s3 operation failed")
	} else {
		ylogger.Zero.Debug().Str("op", "PutObject").Str("bucket", bucket).Str("key", objectPath).Dur("duration", dur).Msg("s3 operation completed")
	}

	putTime := dur.Nanoseconds()
	metrics.StoreLatencyAndSizeInfo("S3_PUT", float64(putLen), float64(putTime))
	return err
}

func (s *S3StorageInteractor) PatchFile(ctx context.Context, name string, r io.ReadSeeker, startOffset int64) error {

	timeStart := time.Now()
	/* XXX: fix usage of default bucket */
	cr := s.credentialMap[s.cnf.StorageBucket]

	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		ylogger.Zero.Err(err).Msg("failed to acquire s3 session")
		return err
	}

	objectPath := strings.TrimLeft(path.Join(s.cnf.StoragePrefix, name), "/")

	ylogger.Zero.Debug().Str("op", "PatchObject").Str("bucket", s.cnf.StorageBucket).Str("key", objectPath).Int64("offset", startOffset).Msg("s3 operation started")

	input := &s3.PatchObjectInput{
		Bucket:       &s.cnf.StorageBucket,
		Key:          aws.String(objectPath),
		Body:         r,
		ContentRange: aws.String(fmt.Sprintf("bytes %d-18446744073709551615", startOffset)),
	}

	opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3StreamTimeout))
	defer cancel()
	_, err = sess.PatchObjectWithContext(opCtx, input)
	dur := time.Since(timeStart)
	if err != nil {
		logContextError(err, "PatchObject", s.cnf.StorageBucket, objectPath, time.Duration(s.cnf.S3StreamTimeout))
		ylogger.Zero.Error().Str("op", "PatchObject").Str("bucket", s.cnf.StorageBucket).Str("key", objectPath).Dur("duration", dur).Err(err).Msg("s3 operation failed")
	} else {
		ylogger.Zero.Debug().Str("op", "PatchObject").Str("bucket", s.cnf.StorageBucket).Str("key", objectPath).Dur("duration", dur).Msg("s3 operation completed")
	}

	return err
}

func (s *S3StorageInteractor) ListPath(ctx context.Context, prefix string, useCache bool, settings []settings.StorageSettings) ([]*object.ObjectInfo, error) {
	if useCache {
		objectMetas, err := readCache(*s.cnf, prefix)
		if err == nil {
			return objectMetas, nil
		}
		ylogger.Zero.Debug().Msg("cache was not found, listing from source bucket")
	}

	tableSpace := ResolveStorageSetting(settings, message.TableSpaceSetting, tablespace.DefaultTableSpace)

	bucket, ok := s.TSToBucketMap[tableSpace]
	if !ok {
		err := fmt.Errorf("failed to match tablespace %s to s3 bucket", tableSpace)
		ylogger.Zero.Err(err)
		return nil, err
	}

	return s.ListBucketPath(ctx, bucket, prefix, useCache)
}

func (s *S3StorageInteractor) ListBucketPath(ctx context.Context, bucket, prefix string, useCache bool) ([]*object.ObjectInfo, error) {

	/* XXX: fix usage of default bucket */
	cr := s.credentialMap[bucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		ylogger.Zero.Err(err).Msg("failed to acquire s3 session")
		return nil, err
	}

	var continuationToken *string
	prefix = strings.TrimLeft(path.Join(s.cnf.StoragePrefix, prefix), "/")
	metas := make([]*object.ObjectInfo, 0)

	ylogger.Zero.Debug().Str("op", "ListObjectsV2").Str("bucket", bucket).Str("prefix", prefix).Msg("s3 operation started")
	listStart := time.Now()

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3OperationTimeout))
		out, err := sess.ListObjectsV2WithContext(opCtx, input)
		cancel()
		if err != nil {
			logContextError(err, "ListObjectsV2", bucket, prefix, time.Duration(s.cnf.S3OperationTimeout))
			ylogger.Zero.Error().Str("op", "ListObjectsV2").Str("bucket", bucket).Str("prefix", prefix).Dur("duration", time.Since(listStart)).Err(err).Msg("s3 operation failed")
			return nil, err
		}

		for _, obj := range out.Contents {
			path := *obj.Key

			cPath, ok := strings.CutPrefix(path, s.cnf.StoragePrefix)
			if !ok {
				ylogger.Zero.Debug().Str("path", path).Msg("skipping file")
				continue
			}
			// this is very hacky, but...
			if cPath[0] != '/' {
				cPath = "/" + cPath
			}
			ylogger.Zero.Debug().Str("path", path).Str("cpath", cPath).Msg("appending file to s3 result")
			metas = append(metas, &object.ObjectInfo{
				Path:    cPath,
				Size:    *obj.Size,
				LastMod: *obj.LastModified,
			})
		}

		if !*out.IsTruncated {
			break
		}

		continuationToken = out.NextContinuationToken
	}

	ylogger.Zero.Debug().Str("op", "ListObjectsV2").Str("bucket", bucket).Str("prefix", prefix).Int("count", len(metas)).Dur("duration", time.Since(listStart)).Msg("s3 operation completed")

	if useCache {
		err = putInCache(s.cnf.ID(), metas)
		if err != nil {
			ylogger.Zero.Debug().Err(err).Msg("failed to put objects in cache")
		}
	}

	return metas, nil
}

func (s *S3StorageInteractor) DeleteObject(ctx context.Context, bucket, key string) error {

	/* XXX: fix usage of default bucket */
	cr := s.credentialMap[bucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		ylogger.Zero.Err(err).Msg("failed to acquire s3 session")
		return err
	}
	if !strings.HasPrefix(key, s.cnf.StoragePrefix) {
		key = path.Join(s.cnf.StoragePrefix, key)
	}
	key = strings.TrimLeft(key, "/")

	ylogger.Zero.Debug().Str("op", "DeleteObject").Str("bucket", bucket).Str("key", key).Msg("s3 operation started")
	deleteStart := time.Now()

	input2 := s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    aws.String(key),
	}

	opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3OperationTimeout))
	defer cancel()
	_, err = sess.DeleteObjectWithContext(opCtx, &input2)
	if err != nil {
		logContextError(err, "DeleteObject", bucket, key, time.Duration(s.cnf.S3OperationTimeout))
		ylogger.Zero.Error().Str("op", "DeleteObject").Str("bucket", bucket).Str("key", key).Dur("duration", time.Since(deleteStart)).Err(err).Msg("s3 operation failed")
		return err
	}
	ylogger.Zero.Debug().Str("op", "DeleteObject").Str("bucket", bucket).Str("key", key).Dur("duration", time.Since(deleteStart)).Msg("s3 operation completed")
	return nil
}

func (s *S3StorageInteractor) SScopyObject(ctx context.Context, from, to, fromStoragePrefix, fromStorageBucket, toStorageBucket string) error {

	/* XXX: fix usage of default bucket */
	cr := s.credentialMap[toStorageBucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		ylogger.Zero.Err(err).Msg("failed to acquire s3 session")
		return err
	}
	if !strings.HasPrefix(from, fromStoragePrefix) {
		from = path.Join(fromStoragePrefix, from)
	}
	from = strings.TrimLeft(from, "/")

	if !strings.HasPrefix(to, s.cnf.StoragePrefix) {
		to = path.Join(s.cnf.StoragePrefix, to)
	}
	to = strings.TrimLeft(to, "/")
	from = path.Join(fromStorageBucket, from)

	ylogger.Zero.Debug().Str("op", "CopyObject").Str("bucket", toStorageBucket).Str("from", from).Str("to", to).Msg("s3 operation started")
	copyStart := time.Now()

	inp := s3.CopyObjectInput{
		Bucket:     &toStorageBucket,
		CopySource: aws.String(from),
		Key:        aws.String(to),
	}

	opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3OperationTimeout))
	defer cancel()
	_, err = sess.CopyObjectWithContext(opCtx, &inp)
	if err != nil {
		logContextError(err, "CopyObject", toStorageBucket, to, time.Duration(s.cnf.S3OperationTimeout))
		ylogger.Zero.Error().Str("op", "CopyObject").Str("bucket", toStorageBucket).Str("from", from).Str("to", to).Dur("duration", time.Since(copyStart)).Err(err).Msg("s3 operation failed")
		return err
	}
	ylogger.Zero.Debug().Str("op", "CopyObject").Str("bucket", toStorageBucket).Str("from", from).Str("to", to).Dur("duration", time.Since(copyStart)).Msg("s3 operation completed")

	return nil
}

func (s *S3StorageInteractor) MoveObject(ctx context.Context, bucket string, from string, to string) error {
	if from == to {
		return nil
	}
	if err := s.SScopyObject(ctx, from, to, s.cnf.StoragePrefix /* same for all buckets */, bucket, bucket); err != nil {
		return err
	}
	return s.DeleteObject(ctx, bucket, from)
}

func (s *S3StorageInteractor) CopyObject(ctx context.Context, from, to, fromStoragePrefix, fromStorageBucket, toStorageBucket string) error {
	return s.SScopyObject(ctx, from, to, fromStoragePrefix, fromStorageBucket, toStorageBucket)
}

func (s *S3StorageInteractor) AbortMultipartUpload(ctx context.Context, bucket, key, uploadId string) error {

	ylogger.Zero.Debug().Str("op", "AbortMultipartUpload").Str("bucket", bucket).Str("key", key).Str("upload_id", uploadId).Msg("s3 operation started")
	abortStart := time.Now()

	/* XXX: fix usage of default bucket */
	cr := s.credentialMap[bucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		return err
	}

	opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3OperationTimeout))
	defer cancel()
	_, err = sess.AbortMultipartUploadWithContext(opCtx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		UploadId: aws.String(uploadId),
		Key:      aws.String(key),
	})
	if err != nil {
		ylogger.Zero.Error().Str("op", "AbortMultipartUpload").Str("bucket", bucket).Str("key", key).Dur("duration", time.Since(abortStart)).Err(err).Msg("s3 operation failed")
	} else {
		ylogger.Zero.Debug().Str("op", "AbortMultipartUpload").Str("bucket", bucket).Str("key", key).Dur("duration", time.Since(abortStart)).Msg("s3 operation completed")
	}
	return err
}

func (s *S3StorageInteractor) ListFailedMultipartUploads(ctx context.Context, bucket string) (map[string]string, error) {
	ylogger.Zero.Debug().Str("op", "ListMultipartUploads").Str("bucket", bucket).Msg("s3 operation started")
	listStart := time.Now()

	/* XXX: fix usage of default bucket */
	cr := s.credentialMap[bucket]
	sess, err := s.pool.GetSession(ctx, &cr)
	if err != nil {
		return nil, err
	}

	uploads := make([]*s3.MultipartUpload, 0)
	var keyMarker *string
	for {
		opCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cnf.S3OperationTimeout))
		out, err := sess.ListMultipartUploadsWithContext(opCtx, &s3.ListMultipartUploadsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: keyMarker,
		})
		cancel()
		if err != nil {
			ylogger.Zero.Error().Str("op", "ListMultipartUploads").Str("bucket", bucket).Dur("duration", time.Since(listStart)).Err(err).Msg("s3 operation failed")
			return nil, err
		}

		uploads = append(uploads, out.Uploads...)

		if !*out.IsTruncated {
			break
		}

		keyMarker = out.NextKeyMarker
	}

	out := make(map[string]string)
	for _, upload := range uploads {
		if _, ok := s.multipartUploads.Load(*upload.Key); !ok {
			out[*upload.Key] = *upload.UploadId
		}
	}
	ylogger.Zero.Debug().Str("op", "ListMultipartUploads").Str("bucket", bucket).Int("count", len(out)).Dur("duration", time.Since(listStart)).Msg("s3 operation completed")
	return out, nil
}

// logContextError logs context-related errors (deadline exceeded, cancelled) with operation details.
func logContextError(err error, op, bucket, key string, timeout time.Duration) {
	if errors.Is(err, context.DeadlineExceeded) {
		ylogger.Zero.Error().Str("op", op).Str("bucket", bucket).Str("key", key).Dur("timeout", timeout).Msg("s3 operation timed out")
		metrics.S3CancelledTotal.With(map[string]string{"reason": "timeout"}).Inc()
	} else if errors.Is(err, context.Canceled) {
		ylogger.Zero.Warn().Str("op", op).Str("bucket", bucket).Str("key", key).Msg("s3 operation cancelled (client disconnected)")
		metrics.S3CancelledTotal.With(map[string]string{"reason": "context_cancelled"}).Inc()
	}
}

type cacheEntry struct {
	Objects []*object.ObjectInfo `json:"objects"`
	Time    time.Time            `json:"time"`
}

func putInCache(storageId string, objs []*object.ObjectInfo) error {
	cachePath := config.InstanceConfig().ProxyCnf.BucketCachePath
	if cachePath == "" {
		return fmt.Errorf("cache path is not specified")
	}

	f, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	content, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	cache := map[string]cacheEntry{}
	if len(content) != 0 {
		if err := json.Unmarshal(content, &cache); err != nil {
			return err
		}
	}

	cache[storageId] = cacheEntry{
		Objects: objs,
		Time:    time.Now(),
	}

	content, err = json.Marshal(cache)
	if err != nil {
		return err
	}
	err = f.Truncate(0)
	if err != nil {
		return err
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = f.Write(content)
	return err
}

func readCache(cfg config.Storage, prefix string) ([]*object.ObjectInfo, error) {
	prefix = path.Join("/", cfg.StoragePrefix, prefix)
	cachePath := config.InstanceConfig().ProxyCnf.BucketCachePath
	if cachePath == "" {
		return nil, fmt.Errorf("cache path is not specified")
	}

	f, err := os.Open(cachePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	content, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	objs := map[string]cacheEntry{}
	if err := json.Unmarshal(content, &objs); err != nil {
		return nil, err
	}

	storageFiles, exists := objs[cfg.ID()]
	if !exists {
		return nil, fmt.Errorf("no cache for storage %s", cfg.ID())
	}
	if storageFiles.Time.Before(time.Now().Add(-24 * time.Hour)) {
		return nil, fmt.Errorf("cache for storage %s has expired", cfg.ID())
	}

	res := make([]*object.ObjectInfo, 0, len(storageFiles.Objects))
	for _, obj := range storageFiles.Objects {
		if strings.HasPrefix(obj.Path, prefix) {
			res = append(res, obj)
		}
	}

	return res, err
}
