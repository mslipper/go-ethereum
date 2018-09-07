package ethdb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
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
	BatchConcurrency = 5
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
	log.Debug("Writing key.", "key", hexutil.Encode(key), "valuelen", len(value))

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
		writes: make(map[string][]byte),
		svc: d.svc,
	}
}

type DynamoBatch struct {
	writes map[string][]byte
	size int
	svc *dynamodb.DynamoDB
}

type queue struct {
	items []string
	mtx sync.Mutex
}

func (q *queue) PopItems(n int) []string {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if len(q.items) == 0 {
		return nil
	}

	var out []string
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
	k := string(common.CopyBytes(key))
	val := common.CopyBytes(value)
	b.writes[k] = val
	b.size += len(value)
	return nil
}

func (b *DynamoBatch) Delete(key []byte) error {
	k := string(key)
	val, ok := b.writes[k]
	if !ok {
		return errors.New("key not found")
	}

	delete(b.writes, string(key))
	b.size -= len(val)
	return nil
}

func (b *DynamoBatch) ValueSize() int {
	return b.size
}

func (b *DynamoBatch) Write() error {
	keys := make([]string, len(b.writes))
	i := 0
	for k := range b.writes {
		keys[i] = k
		i++
	}

	var wg sync.WaitGroup
	wg.Add(BatchConcurrency)
	q := &queue{
		items: keys,
	}

	size := q.Size()

	var executors int
	if size > BatchConcurrency * BatchSize {
		executors = BatchConcurrency
	} else {
		executors = int(math.Ceil(float64(size) / BatchConcurrency))
	}

	for i := 0; i < executors; i++ {
		go b.executeWrite(q, &wg)
	}

	wg.Wait()
	log.Info("Wrote batch.", "size", size)
	return nil
}

func (b *DynamoBatch) executeWrite(queue *queue, wg *sync.WaitGroup) {
	keys := queue.PopItems(BatchSize)
	for keys != nil {
		var reqs []*dynamodb.WriteRequest

		for _, key := range keys {
			keyB := []byte(key)
			value := b.writes[key]
			item := keyAttrs(keyB)
			item[ValueKey] = &dynamodb.AttributeValue{
				B: value,
			}
			reqs = append(reqs, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: item,
				},
			})
		}

		reqItems := make(map[string][]*dynamodb.WriteRequest)
		reqItems[TableName] = reqs

		_, err := b.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: reqItems,
		})

		if err != nil {
			panic(err)
		}

		keys = queue.PopItems(BatchSize)
	}

	wg.Done()
}

func (b *DynamoBatch) Reset() {
	b.writes = make(map[string][]byte)
	b.size = 0
}

func keyAttrs(key []byte) map[string]*dynamodb.AttributeValue {
	out := make(map[string]*dynamodb.AttributeValue)
	out[StoreKey] = &dynamodb.AttributeValue{
		B: key,
	}
	return out
}
