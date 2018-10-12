package storage

import (
	"sync"
	"time"
	"github.com/kyokan/levelsrv/pkg"
	"github.com/inconshreveable/log15"
	"bytes"
	"errors"
)

type WriteBehindCache struct {
	backend       Store
	cache         map[string][]byte
	cacheSize     int
	duplicates    int
	mtx           sync.RWMutex
	quitChan      chan bool
	log           log15.Logger
	deletionSigil []byte
}

const FlushCacheSize = 100 * 1024 * 1024

func NewWriteBehindCache(backend Store) (Store, error) {
	log := pkg.NewLogger("cache")

	c := &WriteBehindCache{
		backend:       backend,
		cache:         make(map[string][]byte),
		quitChan:      make(chan bool),
		log:           log,
		deletionSigil: pkg.Rand32(),
	}
	go c.manage()
	return c, nil
}

func (c *WriteBehindCache) Put(key []byte, value []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	_, has := c.cache[string(key)]
	if has {
		c.duplicates++
	}

	c.cache[string(key)] = value
	c.cacheSize += len(value)
	return nil
}

func (c *WriteBehindCache) Delete(key []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	val, has := c.cache[string(key)]
	if has {
		c.cacheSize -= len(val)
		c.duplicates++
	}
	c.cache[string(key)] = c.deletionSigil
	return nil
}

func (c *WriteBehindCache) Get(key []byte) ([]byte, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	val, has := c.cache[string(key)]
	if has {
		if bytes.Equal(val, c.deletionSigil) {
			return nil, errors.New("not found")
		}

		return val, nil
	}

	return c.backend.Get(key)
}

func (c *WriteBehindCache) Has(key []byte) (bool, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	val, has := c.cache[string(key)]
	if has {
		return !bytes.Equal(val, c.deletionSigil), nil
	}

	return c.backend.Has(key)
}

func (c *WriteBehindCache) Close() {
	c.quitChan <- true
}

func (c *WriteBehindCache) NewBatch() Batch {
	return NewMembatch(func(kvs []KV) error {
		c.mtx.Lock()
		defer c.mtx.Unlock()

		for _, kv := range kvs {
			val, has := c.cache[string(kv.K)]

			if kv.IsDel {
				if has {
					c.cacheSize -= len(val)
					c.duplicates++
				}
				c.cache[string(kv.K)] = c.deletionSigil
			} else {
				if has {
					c.duplicates++
				}

				c.cache[string(kv.K)] = kv.V
				c.cacheSize += len(val)
			}
		}

		return nil
	})
}

func (c *WriteBehindCache) manage() {
	tick := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-tick.C:
			c.flush(false)
		case <-c.quitChan:
			c.flush(true)
			c.backend.Close()
			return
		}
	}
}

func (c *WriteBehindCache) flush(force bool) {
	start := time.Now()
	c.mtx.Lock()
	defer c.mtx.Unlock()
	size := c.cacheSize
	if !force && c.cacheSize < FlushCacheSize {
		log15.Info("cache size below flush threshold", "size", pkg.PrettySize(size), "threshold", pkg.PrettySize(FlushCacheSize))
		return
	}

	cache := c.cache
	dups := c.duplicates
	c.cache = make(map[string][]byte)
	c.cacheSize = 0
	c.duplicates = 0
	batch := c.backend.NewBatch()
	for k, v := range cache {
		if bytes.Equal(v, c.deletionSigil) {
			batch.Delete([]byte(k))
		} else {
			batch.Put([]byte(k), v)
		}
	}
	if err := batch.Write(); err != nil {
		c.log.Error("failed to flush write behind cache", "err", err)
		return
	}
	duration := time.Since(start)
	c.log.Info(
		"flushed write-behind cache to disk",
		"size",
		pkg.PrettySize(size),
		"keys",
		len(cache),
		"duplicates",
		dups,
		"elapsed",
		pkg.PrettyDuration(duration),
		"speed",
		pkg.PrettySize(float64(size) / duration.Seconds()).String() + "/s",
	)
}
