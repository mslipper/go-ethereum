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
	)

const (
	TableName   = "Geth-KV"
	StoreKey    = "StoreKey"
	ValueKey    = "Data"
	BatchSize   = 25
)

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
	svc            *dynamodb.DynamoDB
	writeQueue     *queue
	cache          *QueueingCache
	batchesWritten uint
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
		writeQueue: &queue{},
		cache:      NewKeySet(),
	}

	go res.startWriteQueue()
	go res.startQueueMonitor()

	return res, nil
}

func (d *DynamoDatabase) startWriteQueue() {
	for {
		kvs := d.writeQueue.PopItems(BatchSize)

		if kvs == nil {
			log.Trace("Nothing to write, sleeping")
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

		d.batchesWritten += uint(len(reqs))
		log.Trace("Wrote batch", "size", len(reqs), "duration", time.Since(start), "remaining", d.writeQueue.Size())
		kvs = d.writeQueue.PopItems(BatchSize)
	}
}

func (d *DynamoDatabase) startQueueMonitor() {
	ticker := time.NewTicker(1 * time.Minute)

	for range ticker.C {
		log.Info("Batch stats", "written", d.batchesWritten, "size", d.writeQueue.Size())
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
	input := &dynamodb.DeleteItemInput{
		Key:       keyAttrs(key),
		TableName: aws.String(TableName),
	}
	_, err := d.svc.DeleteItem(input)
	d.cache.Delete(key)
	return err
}

func (d *DynamoDatabase) Get(key []byte) ([]byte, error) {
	log.Trace("Getting key.", "key", hexutil.Encode(key))
	has := d.cache.Has(key)

	if !has {
		return nil, nil
	}

	cached := d.cache.Get(key)
	if cached != nil {
		return cached, nil
	}

	input := &dynamodb.GetItemInput{
		Key:            keyAttrs(key),
		TableName:      aws.String(TableName),
		ConsistentRead: aws.Bool(true),
	}
	res, err := d.svc.GetItem(input)
	if err != nil {
		return nil, err
	}

	if res.Item == nil {
		return nil, nil
	}

	val := res.Item[ValueKey].B
	d.cache.Set(key, val)
	return val, nil
}

func (d *DynamoDatabase) Has(key []byte) (bool, error) {
	return d.cache.Has(key), nil
}

func (d *DynamoDatabase) Close() {
}

func (d *DynamoDatabase) enqueue(items []kv) {
	for _, item := range items {
		if item.del {
			d.cache.Delete(item.k)
		} else {
			d.cache.Set(item.k, item.v)
		}
	}

	d.writeQueue.PushItems(items)
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
