package ethdb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/ethereum/go-ethereum/common"
		"sync"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"math"
)

const (
	TableName = "Geth-KV"
	StoreKey = "StoreKey"
	ValueKey = "Data"
	BatchSize = 25
	BatchConcurrency = 10
)

type DynamoDatabase struct {
	svc *dynamodb.DynamoDB
}

func NewDynamoDatabase() (Database, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})

	if err != nil {
		return nil, err
	}

	svc := dynamodb.New(sess)

	return &DynamoDatabase{
		svc: svc,
	}, nil
}

func (d *DynamoDatabase) Put(key []byte, value []byte) error {
	log.Trace("Writing key.", "key", hexutil.Encode(key), "valuelen", len(value))

	if len(value) == 0 {
		value = []byte{0x00}
	}

	item := keyAttrs(key)
	item[ValueKey] = &dynamodb.AttributeValue{
		B: value,
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(TableName),
	}

	_, err := d.svc.PutItem(input)
	return err
}

func (d *DynamoDatabase) Delete(key []byte) error {
	log.Trace("Deleting key.", "key", hexutil.Encode(key))
	input := &dynamodb.DeleteItemInput{
		Key:       keyAttrs(key),
		TableName: aws.String(TableName),
	}
	_, err := d.svc.DeleteItem(input)
	return err
}

func (d *DynamoDatabase) Get(key []byte) ([]byte, error) {
	log.Trace("Getting key.", "key", hexutil.Encode(key))
	input := &dynamodb.GetItemInput{
		Key:       keyAttrs(key),
		TableName: aws.String(TableName),
		ConsistentRead: aws.Bool(true),
	}
	res, err := d.svc.GetItem(input)
	if err != nil {
		return nil, err
	}

	if res.Item == nil {
		return nil, nil
	}

	return res.Item[ValueKey].B, nil
}

func (d *DynamoDatabase) Has(key []byte) (bool, error) {
	res, err := d.Get(key)
	if err != nil {
		return false, err
	}

	return res != nil, nil
}

func (d *DynamoDatabase) Close() {
}

func (d *DynamoDatabase) NewBatch() Batch {
	return &DynamoBatch{
		svc: d.svc,
	}
}

type DynamoBatch struct {
	writes []kv
	size int
	svc *dynamodb.DynamoDB
}

type queue struct {
	items []kv
	mtx sync.Mutex
}

func (q *queue) PopItems(n int) []kv {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if len(q.items) == 0 {
		return nil
	}

	var out []kv
	for len(q.items) > 0 && len(out) < n {
		idx := len(q.items) - 1
		item := q.items[idx]
		q.items = q.items[:idx]
		out = append(out, item)
	}

	return out
}

func (q *queue) Size() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return len(q.items)
}

func (b *DynamoBatch) Put(key []byte, value []byte) error {
	log.Trace("Staging batch write.", "key", hexutil.Encode(key))
	k := common.CopyBytes(key)
	v := common.CopyBytes(value)

	kv := kv{
		k: k,
		v: v,
	}

	b.writes = append(b.writes, kv)
	b.size += len(value)
	return nil
}

func (b *DynamoBatch) Delete(key []byte) error {
	log.Debug("Staging batch delete.", "key", hexutil.Encode(key))
	k := common.CopyBytes(key)
	kv := kv {
		k: k,
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
		log.Debug("Writing batch length of zero, bailing.")
		panic("oh no")
		return nil
	}

	var wg sync.WaitGroup
	q := &queue{
		items: b.writes[:],
	}
	size := q.Size()

	var executors int
	if size > BatchConcurrency * BatchSize {
		executors = BatchConcurrency
	} else {
		executors = int(math.Ceil(float64(size) / BatchConcurrency))
	}
	wg.Add(executors)
	log.Debug("Waiting on batch executors.", "count", executors)

	for i := 0; i < executors; i++ {
		go b.executeWrite(q, &wg)
	}

	wg.Wait()
	log.Info("Wrote batch.", "size", size)
	return nil
}

func (b *DynamoBatch) executeWrite(queue *queue, wg *sync.WaitGroup) {
	kvs := queue.PopItems(BatchSize)
	for kvs != nil {
		var reqs []*dynamodb.WriteRequest

		for _, kv := range kvs {
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

		_, err := b.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: reqItems,
		})

		if err != nil {
			panic(err)
		}

		log.Trace("Wrote batch.", "size", BatchSize)
		kvs = queue.PopItems(BatchSize)
	}

	wg.Done()
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
