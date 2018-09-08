package ethdb

import (
						"github.com/ethereum/go-ethereum/log"
			"time"
	"github.com/allegro/bigcache"
)

type KeySet struct {
	cache       *bigcache.BigCache
}

var empty []byte

func NewKeySet() *KeySet {
	config := bigcache.Config{
		Shards: 1024,
		LifeWindow:       24 * time.Hour,
	}
	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		panic(err)
	}

	res := &KeySet{
		cache: cache,
	}

	go func() {
		tick := time.NewTicker(1 * time.Minute)

		for {
			select {
			case <-tick.C:
				log.Info(
					"Cache metrics",
					"size",
					res.cache.Len(),
				)
			}
		}
	}()

	return res
}

func (s *KeySet) Add(key []byte) {
	err := s.cache.Set(string(key), empty)
	if err != nil {
		panic(err)
	}
}

func (s *KeySet) Remove(key []byte) {
	err := s.cache.Delete(string(key))
	if err != nil {
		_, ok := err.(*bigcache.EntryNotFoundError)
		if !ok {
			panic(err)
		}
	}
}

func (s *KeySet) Has(key []byte) bool {
	_, err := s.cache.Get(string(key))
	if err != nil {
		_, ok := err.(*bigcache.EntryNotFoundError)
		if !ok {
			panic(err)
		}

		return false
	}

	return true
}