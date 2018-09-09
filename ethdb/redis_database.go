package ethdb

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"crypto/sha1"
	"github.com/go-redis/redis"
	"bytes"
	"compress/gzip"
	"io"
)

type RedisDatabase struct {
	rClient    *redis.Client
	writeQueue *queue
	cache      *QueueingCache
	flushMtx   sync.Mutex
	idleChan   chan struct{}
}

func NewRedisDatabase() (Database, error) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	log.Info("Redis connected")

	res := &RedisDatabase{
		rClient: client,
	}

	return res, nil
}

func (d *RedisDatabase) Put(key []byte, value []byte) error {
	k := shaKey(key)
	log.Trace("Writing key", "key", k, "valuelen", len(value))
	compressed, err := compress(value)
	if err != nil {
		return err
	}
	return d.rClient.Set(k, compressed, 0).Err()
}

func (d *RedisDatabase) Delete(key []byte) error {
	k := shaKey(key)
	log.Trace("Deleting key", "key", k)
	return d.rClient.Del(k).Err()
}

func (d *RedisDatabase) Get(key []byte) ([]byte, error) {
	k := shaKey(key)
	log.Trace("Getting key", "key", k)
	res, err := d.rClient.Get(k).Bytes()
	if err != nil {
		return nil, err
	}
	return decompress(res)
}

func (d *RedisDatabase) Has(key []byte) (bool, error) {
	res, err := d.rClient.Exists(shaKey(key)).Result()
	return res == 1, err
}

func (d *RedisDatabase) Close() {
	err := d.rClient.Close()
	if err != nil {
		log.Crit("error closing redis", err)
	}
}

func (d *RedisDatabase) NewBatch() Batch {
	return &RedisBatch{
		db: d,
	}
}

type RedisBatch struct {
	writes []kv
	size   int
	db     *RedisDatabase
}

func (b *RedisBatch) Put(key []byte, value []byte) error {
	log.Trace("Staging batch write.", "key", hexutil.Encode(key))
	k := common.CopyBytes(key)
	var v []byte
	if len(value) == 0 {
		v = []byte{0x00}
	} else {
		v = common.CopyBytes(value)
	}

	kv := kv{
		k: k,
		v: v,
	}

	b.writes = append(b.writes, kv)
	b.size += len(value)
	return nil
}

func (b *RedisBatch) Delete(key []byte) error {
	log.Info("Staging batch delete.", "key", hexutil.Encode(key))
	k := common.CopyBytes(key)
	kv := kv{
		k:   k,
		del: true,
	}
	b.writes = append(b.writes, kv)
	b.size += 1
	return nil
}

func (b *RedisBatch) ValueSize() int {
	return b.size
}

func (b *RedisBatch) Write() error {
	writeLen := len(b.writes)

	if writeLen == 0 {
		return errors.New("batch length is zero")
	}

	pipe := b.db.rClient.Pipeline()

	for _, kv := range b.writes {
		k := shaKey(kv.k)
		if kv.del {
			pipe.Del(k)
		} else {
			compressed, err := compress(kv.v)
			if err != nil {
				return err
			}

			pipe.Set(k, compressed, 0)
		}
	}

	_, err := pipe.Exec()
	return err
}

func (b *RedisBatch) Reset() {
	b.writes = make([]kv, 0)
	b.size = 0
}

func shaKey(key []byte) string {
	sha := sha1.Sum(key)
	return hexutil.Encode(sha[:])
}

func compress(value []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(value); err != nil {
		return nil, err
	}
	if err := gz.Flush(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decompress(value []byte) ([]byte, error) {
	var b bytes.Buffer
	gz, err := gzip.NewReader(bytes.NewReader(value))
	if err != nil {
		return nil, err
	}
	io.Copy(&b, gz)
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}