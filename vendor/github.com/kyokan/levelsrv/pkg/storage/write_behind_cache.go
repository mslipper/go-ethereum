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
	backendBatch  Batch
	flushCapacity int
	forceTick     <-chan time.Time
	checkTick     <-chan time.Time
}

const DefaultFlushCapacity = 100 * 1024 * 1024

type WBCOption func(c *WriteBehindCache)

func WithFlushCapacity(capacity int) WBCOption {
	return func(c *WriteBehindCache) {
		c.flushCapacity = capacity
	}
}

func WithForceTick(ch <-chan time.Time) WBCOption {
	return func(c *WriteBehindCache) {
		c.forceTick = ch
	}
}

func WithCheckTick(ch <-chan time.Time) WBCOption {
	return func(c *WriteBehindCache) {
		c.checkTick = ch
	}
}

func NewWriteBehindCache(backend Store, options ...WBCOption) (Store, error) {
	log := pkg.NewLogger("cache")

	c := &WriteBehindCache{
		backend:       backend,
		cache:         make(map[string][]byte),
		quitChan:      make(chan bool),
		log:           log,
		deletionSigil: pkg.Rand32(),
		backendBatch:  backend.NewBatch(),
	}

	for _, opt := range options {
		opt(c)
	}

	if c.flushCapacity == 0 {
		c.flushCapacity = DefaultFlushCapacity
	}
	if c.checkTick == nil {
		checkTick := time.NewTicker(1 * time.Second)
		c.checkTick = checkTick.C
	}
	if c.forceTick == nil {
		forceTick := time.NewTicker(1 * time.Minute)
		c.forceTick = forceTick.C
	}

	go c.manage()
	return c, nil
}

func (c *WriteBehindCache) Put(key []byte, value []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	old, has := c.cache[string(key)]
	if has {
		c.duplicates++
		c.cacheSize -= len(old)
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
	return NewMembatch(c.batchHandler)
}

func (c *WriteBehindCache) batchHandler(kvs []*KV) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, kv := range kvs {
		old, has := c.cache[string(kv.K)]

		if kv.IsDel {
			if has {
				c.duplicates++
				c.cacheSize -= len(old)
			}
			c.cache[string(kv.K)] = c.deletionSigil
		} else {
			if has {
				c.duplicates++
				c.cacheSize -= len(old)
			}

			c.cache[string(kv.K)] = kv.V
			c.cacheSize += len(kv.V)
		}
	}

	return nil
}

func (c *WriteBehindCache) manage() {
	for {
		select {
		case <-c.checkTick:
			c.flush(false)
		case <-c.forceTick:
			c.flush(true)
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
	if !force && c.cacheSize < c.flushCapacity {
		log15.Info("cache size below flush threshold", "size", pkg.PrettySize(size), "threshold", pkg.PrettySize(c.flushCapacity))
		return
	}
	if force {
		log15.Info("forcing cache flush", "size", pkg.PrettySize(size), "threshold", pkg.PrettySize(c.flushCapacity))
	}

	cache := c.cache
	dups := c.duplicates
	batch := c.backendBatch
	defer batch.Reset()
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
	c.cache = make(map[string][]byte)
	c.cacheSize = 0
	c.duplicates = 0
	duration := time.Since(start)
	c.log.Info(
		"flushed write-behind cache to backend",
		"size",
		pkg.PrettySize(size),
		"keys",
		len(cache),
		"duplicates",
		dups,
		"elapsed",
		pkg.PrettyDuration(duration),
		"speed",
		pkg.PrettySize(float64(size) / duration.Seconds()).String()+"/s",
	)
}
