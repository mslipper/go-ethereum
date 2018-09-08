package ethdb

import (
						"github.com/ethereum/go-ethereum/log"
			"time"
	"github.com/allegro/bigcache"
)

const (
	DataCacheSizeMB = 16 * 1024
)

type QueueingCache struct {
	keys *bigcache.BigCache
	data *bigcache.BigCache
}

var empty []byte

func NewKeySet() *QueueingCache {
	ksConfig := bigcache.Config{
		Shards: 1024,
		LifeWindow:       24 * time.Hour,
	}
	dataConfig := bigcache.Config{
		Shards:           1024,
		LifeWindow:       24 * time.Hour,
		HardMaxCacheSize: DataCacheSizeMB,
	}
	keys, err := bigcache.NewBigCache(ksConfig)
	if err != nil {
		panic(err)
	}
	data, err := bigcache.NewBigCache(dataConfig)
	if err != nil {
		panic(err)
	}

	res := &QueueingCache{
		keys: keys,
		data: data,
	}

	go func() {
		tick := time.NewTicker(1 * time.Minute)

		for {
			select {
			case <-tick.C:
				log.Info(
					"Cache metrics",
					"keys",
					res.keys.Len(),
					"itemsize",
					res.data.Capacity(),
				)
			}
		}
	}()

	return res
}

func (s *QueueingCache) Set(key []byte, value []byte) {
	err := s.keys.Set(string(key), empty)
	if err != nil {
		panic(err)
	}
	err = s.data.Set(string(key), value)
	if err != nil {
		panic(err)
	}
}

func (s *QueueingCache) Delete(key []byte) {
	err := s.keys.Delete(string(key))
	if err != nil {
		_, ok := err.(*bigcache.EntryNotFoundError)
		if !ok {
			panic(err)
		}
	}

	err = s.data.Delete(string(key))
	if err != nil {
		panic(err)
	}
}

func (s *QueueingCache) Has(key []byte) bool {
	_, err := s.keys.Get(string(key))
	if err != nil {
		_, ok := err.(*bigcache.EntryNotFoundError)
		if !ok {
			panic(err)
		}

		return false
	}

	return true
}

func (s *QueueingCache) Get(key []byte) []byte {
	if !s.Has(key) {
		return nil
	}

	val, err := s.data.Get(string(key))
	if err != nil {
		panic(err)
	}

	return val
}