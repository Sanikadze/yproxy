package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/yezzey-gp/yproxy/config"
	"github.com/yezzey-gp/yproxy/pkg/object"
	"github.com/yezzey-gp/yproxy/pkg/settings"
	"github.com/yezzey-gp/yproxy/pkg/tablespace"
)

type StorageReader interface {
	CatFileFromStorage(ctx context.Context, name string, offset int64, setts []settings.StorageSettings) (io.ReadCloser, error)
}

type StorageWriter interface {
	PutFileToDest(ctx context.Context, name string, r io.Reader, settings []settings.StorageSettings) error
	PatchFile(ctx context.Context, name string, r io.ReadSeeker, startOffset int64) error
}

type StorageLister interface {
	ListPath(ctx context.Context, prefix string, useCache bool, settings []settings.StorageSettings) ([]*object.ObjectInfo, error)
	ListBucketPath(ctx context.Context, bucket, prefix string, useCache bool) ([]*object.ObjectInfo, error)
	ListFailedMultipartUploads(ctx context.Context, bucket string) (map[string]string, error)
}

type StorageMover interface {
	MoveObject(ctx context.Context, bucket, from string, to string) error
	DeleteObject(ctx context.Context, bucket, key string) error
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadId string) error
}

type StorageCopier interface {
	CopyObject(ctx context.Context, from, to, fromStoragePrefix, fromStorageBucket, toStorageBucket string) error
}

//go:generate mockgen -destination=pkg/mock/storage.go -package=mock
type StorageInteractor interface {
	StorageReader
	StorageWriter
	StorageLister
	StorageMover
	StorageCopier

	ListBuckets() []string
	DefaultBucket() string
}

func NewStorage(cnf *config.Storage, storageName string) (StorageInteractor, error) {
	switch cnf.StorageType {
	case "fs":
		return &FileStorageInteractor{
			cnf: cnf,
		}, nil
	case "s3":
		return &S3StorageInteractor{
			pool:          NewSessionPool(cnf, storageName),
			cnf:           cnf,
			TSToBucketMap: buildBucketMapFromCnf(cnf),
			credentialMap: buildCredMapFromCnf(cnf),
		}, nil
	default:
		return nil, fmt.Errorf("wrong storage type %s", cnf.StorageType)
	}
}

func buildBucketMapFromCnf(cnf *config.Storage) map[string]string {
	mp := cnf.TablespaceMap
	if mp == nil {
		/* fallback for backward-compatibility if to TableSpace map configured */
		mp = map[string]string{}
	}
	if _, ok := mp[tablespace.DefaultTableSpace]; !ok {
		mp[tablespace.DefaultTableSpace] = cnf.StorageBucket
	}
	return mp
}

func buildCredMapFromCnf(cnf *config.Storage) map[string]config.StorageCredentials {
	mp := cnf.CredentialMap
	if mp == nil {
		/* fallback for backward-compatibility if to TableSpace map configured */
		mp = map[string]config.StorageCredentials{}
	}
	if _, ok := mp[cnf.StorageBucket]; !ok {
		mp[cnf.StorageBucket] = config.StorageCredentials{
			AccessKeyId:     cnf.AccessKeyId,
			SecretAccessKey: cnf.SecretAccessKey,
		}
	}
	return mp
}
