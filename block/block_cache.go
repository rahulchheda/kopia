package block

import (
	"context"
	"os"
	"time"

	"github.com/kopia/kopia/storage/filesystem"

	"github.com/kopia/kopia/storage"
)

type blockCache interface {
	getBlock(virtualBlockID, physicalBlockID string, offset, length int64) ([]byte, error)
	putBlock(blockID string, data []byte) error
	listIndexBlocks(full bool) ([]Info, error)
	close() error
}

// CachingOptions specifies configuration of local cache.
type CachingOptions struct {
	CacheDirectory          string `json:"cacheDirectory,omitempty"`
	MaxCacheSizeBytes       int64  `json:"maxCacheSize,omitempty"`
	MaxListCacheDurationSec int    `json:"maxListCacheDuration,omitempty"`
	IgnoreListCache         bool   `json:"-"`
	HMACSecret              []byte `json:"-"`
}

func newBlockCache(st storage.Storage, caching CachingOptions) (blockCache, error) {
	if caching.MaxCacheSizeBytes == 0 || caching.CacheDirectory == "" {
		return nullBlockCache{st}, nil
	}

	os.MkdirAll(caching.CacheDirectory, 0755)
	cacheStorage, err := filesystem.New(context.Background(), &filesystem.Options{
		Path: caching.CacheDirectory,
	})
	if err != nil {
		return nil, err
	}

	c := &diskBlockCache{
		st:                st,
		cacheStorage:      cacheStorage,
		maxSizeBytes:      caching.MaxCacheSizeBytes,
		hmacSecret:        append([]byte(nil), caching.HMACSecret...),
		listCacheDuration: time.Duration(caching.MaxListCacheDurationSec) * time.Second,
		closed:            make(chan struct{}),
	}

	if caching.IgnoreListCache {
		c.deleteListCache()
	}

	if err := c.sweepDirectory(); err != nil {
		return nil, err
	}
	go c.sweepDirectoryPeriodically()

	return c, nil
}
