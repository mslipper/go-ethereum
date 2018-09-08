package ethdb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/ethereum/go-ethereum/common"
	"sync"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"time"
	"github.com/allegro/bigcache"
)

const (
	TableName   = "Geth-KV"
	StoreKey    = "StoreKey"
	ValueKey    = "Data"
	BatchSize   = 25
	CacheSizeMB = 2048
	Megabyte    = 1000000
)

type dynamoCache struct {
	cacheHits   uint
	cacheMisses uint
	cache       *bigcache.BigCache
}

func NewDynamoCache() *dynamoCache {
	config := bigcache.Config{
		Shards:           1024,
		LifeWindow:       24 * time.Hour,
		HardMaxCacheSize: CacheSizeMB,
	}

	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		panic(err)
	}

	res := &dynamoCache{
		cache: cache,
	}

	go func() {
		tick := time.NewTicker(1 * time.Minute)

		for {
			select {
			case <-tick.C:
				log.Info(
					"Cache metrics",
					"hits",
					res.cacheHits,
					"misses",
					res.cacheMisses,
					"rate",
					(float64(res.cacheHits)/float64(res.cacheMisses+res.cacheHits))*100,
					"size",
					res.cache.Len(),
				)
			}
		}
	}()

	return res
}

func (c *dynamoCache) Size() int {
	return c.cache.Capacity()
}

func (c *dynamoCache) Flush() {
	err := c.cache.Reset()
	if err != nil {
		panic(err)
	}
}

func (c *dynamoCache) Set(key []byte, value []byte) {
	err := c.cache.Set(string(key), value)
	if err != nil {
		panic(err)
	}
}

func (c *dynamoCache) Get(key []byte) []byte {
	res, err := c.cache.Get(string(key))
	if err != nil {
		_, ok := err.(*bigcache.EntryNotFoundError)
		if !ok {
			panic(err)
		}

		c.cacheMisses++
		return nil
	}
	c.cacheHits++
	return res
}

func (c *dynamoCache) Delete(key []byte) {
	err := c.cache.Delete(string(key))
	if err != nil {
		_, ok := err.(*bigcache.EntryNotFoundError)
		if !ok {
			panic(err)
		}
	}
}

type queue struct {
	items []kv
	mtx   sync.Mutex
}

func (q *queue) PushItems(items []kv) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if len(items) == 0 {
		return
	}

	q.items = append(q.items, items...)
}

func (q *queue) PopItems(n int) []kv {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if len(q.items) == 0 {
		return nil
	}

	var out []kv
	for len(q.items) > 0 && len(out) < n {
		idx := 0
		item := q.items[idx]
		q.items = q.items[1:]
		out = append(out, item)
	}

	return out
}

func (q *queue) Size() int {
	return len(q.items)
}

type DynamoDatabase struct {
	svc        *dynamodb.DynamoDB
	writeQueue *queue
	cache      *dynamoCache
	flushing   bool
	putMtx     sync.Mutex
	empty      chan struct{}
}

func NewDynamoDatabase() (Database, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})

	if err != nil {
		return nil, err
	}

	svc := dynamodb.New(sess)

	res := &DynamoDatabase{
		svc:        svc,
		cache:      NewDynamoCache(),
		writeQueue: &queue{},
		empty:      make(chan struct{}),
	}

	//go res.startCacheWatcher()
	//go res.startWriteQueue()

	return res, nil
}

func (d *DynamoDatabase) startWriteQueue() {
	for {
		kvs := d.writeQueue.PopItems(BatchSize)

		if kvs == nil {
			log.Trace("Nothing to write, sleeping")
			d.empty <- struct{}{}
			time.Sleep(1 * time.Second)
			continue
		}

		start := time.Now()
		var uniqueKvs []kv
		usedKeys := make(map[string]bool)
		for i := 0; i < len(kvs); i++ {
			kv := kvs[i]
			keyStr := string(kv.k)
			if usedKeys[keyStr] {
				break
			}

			usedKeys[keyStr] = true
			uniqueKvs = append(uniqueKvs, kv)
		}

		var reqs []*dynamodb.WriteRequest
		for _, kv := range uniqueKvs {
			k := kv.k
			v := kv.v
			del := kv.del
			req := &dynamodb.WriteRequest{}
			item := keyAttrs(k)

			if del {
				req.DeleteRequest = &dynamodb.DeleteRequest{
					Key: item,
				}
			} else {
				item[ValueKey] = &dynamodb.AttributeValue{
					B: v,
				}
				req.PutRequest = &dynamodb.PutRequest{
					Item: item,
				}
			}

			reqs = append(reqs, req)
			log.Trace("Preparing batch write.", "kv", hexutil.Encode(k))
		}

		reqItems := make(map[string][]*dynamodb.WriteRequest)
		reqItems[TableName] = reqs

		_, err := d.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: reqItems,
		})

		if err != nil {
			panic(err)
		}

		log.Trace("Wrote batch", "size", len(reqs), "duration", time.Since(start), "remaining", d.writeQueue.Size())
		kvs = d.writeQueue.PopItems(BatchSize)
	}
}

func (d *DynamoDatabase) startCacheWatcher() {
	tick := time.NewTicker(5 * time.Minute)

	for {
		select {
		case <-tick.C:
			utilization := float64(d.cache.Size()) / float64(CacheSizeMB*Megabyte)
			if utilization > 0.85 {
				log.Info("Utilization above 85%, flushing cache and writes", "utilization", utilization)
				d.putMtx.Lock()
				<-d.empty
				d.cache.Flush()
				d.putMtx.Unlock()
			}
		case <-d.empty:
			continue
		}
	}
}

func (d *DynamoDatabase) Put(key []byte, value []byte) error {
	log.Trace("Writing key.", "key", hexutil.Encode(key), "valuelen", len(value))

	if len(value) == 0 {
		value = []byte{0x00}
	}

	item := kv{
		k: key,
		v: value,
	}

	d.enqueue([]kv{item})
	return nil
}

func (d *DynamoDatabase) Delete(key []byte) error {
	log.Trace("Deleting key.", "key", hexutil.Encode(key))
	//input := &dynamodb.DeleteItemInput{
	//	Key:       keyAttrs(key),
	//	TableName: aws.String(TableName),
	//}
	//_, err := d.svc.DeleteItem(input)
	d.cache.Delete(key)
	//return err
	return nil
}

func (d *DynamoDatabase) Get(key []byte) ([]byte, error) {
	log.Trace("Getting key.", "key", hexutil.Encode(key))
	cached := d.cache.Get(key)
	return cached, nil

	//if cached != nil {
	//	return cached, nil
	//}
	//
	//log.Info("Cache miss")
	//input := &dynamodb.GetItemInput{
	//	Key:            keyAttrs(key),
	//	TableName:      aws.String(TableName),
	//	ConsistentRead: aws.Bool(true),
	//}
	//
	//res, err := d.svc.GetItem(input)
	//if err != nil {
	//	return nil, err
	//}
	//
	//if res.Item == nil {
	//	return nil, nil
	//}
	//
	//val := res.Item[ValueKey].B
	//d.cache.Set(key, val)
	//return val, nil
}

func (d *DynamoDatabase) Has(key []byte) (bool, error) {
	d.putMtx.Lock()
	defer d.putMtx.Unlock()
	res, err := d.Get(key)
	if err != nil {
		return false, err
	}

	return res != nil, nil
}

func (d *DynamoDatabase) Close() {
}

func (d *DynamoDatabase) enqueue(items []kv) {
	d.putMtx.Lock()
	defer d.putMtx.Unlock()

	for _, item := range items {
		if item.del {
			d.cache.Delete(item.k)
		} else {
			d.cache.Set(item.k, item.v)
		}
	}

	//d.writeQueue.PushItems(items)
}

func (d *DynamoDatabase) NewBatch() Batch {
	return &DynamoBatch{
		db: d,
	}
}

type DynamoBatch struct {
	writes []kv
	size   int
	db     *DynamoDatabase
}

func (b *DynamoBatch) Put(key []byte, value []byte) error {
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

func (b *DynamoBatch) Delete(key []byte) error {
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

func (b *DynamoBatch) ValueSize() int {
	return b.size
}

func (b *DynamoBatch) Write() error {
	writeLen := len(b.writes)

	if writeLen == 0 {
		return errors.New("batch length is zero")
	}

	b.db.enqueue(b.writes)
	return nil
}

func (b *DynamoBatch) Reset() {
	b.writes = make([]kv, 0)
	b.size = 0
}

func keyAttrs(key []byte) map[string]*dynamodb.AttributeValue {
	out := make(map[string]*dynamodb.AttributeValue)
	out[StoreKey] = &dynamodb.AttributeValue{
		B: key,
	}
	return out
}
