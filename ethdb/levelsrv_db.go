package ethdb

import (
	"github.com/kyokan/levelsrv/pkg/levelsrv"
	"context"
	"github.com/kyokan/levelsrv/pkg/storage"
)

type LevelSRVDatabase struct {
	client *levelsrv.Client
}

func NewLevelSRVDatabase(url string) (*LevelSRVDatabase, error) {
	client, err := levelsrv.NewClient(context.TODO(), url)
	if err != nil {
		return nil, err
	}

	return &LevelSRVDatabase{
		client: client,
	}, nil
}

func (l *LevelSRVDatabase) Put(key []byte, value []byte) error {
	return l.client.Put(key, value)
}

func (l *LevelSRVDatabase) Delete(key []byte) error {
	return l.client.Delete(key)
}

func (l *LevelSRVDatabase) Get(key []byte) ([]byte, error) {
	return l.client.Get(key)
}

func (l *LevelSRVDatabase) Has(key []byte) (bool, error) {
	return l.client.Has(key)
}

func (l *LevelSRVDatabase) Close() {
	if err := l.client.Close(); err != nil {
		panic(err)
	}
}

func (l *LevelSRVDatabase) NewBatch() Batch {
	return &lsrvBatch{
		client: l.client,
		lBatch: l.client.NewBatch(),
	}
}

type lsrvBatch struct {
	client *levelsrv.Client
	lBatch storage.Batch
	size int
}

func (l *lsrvBatch) Put(key []byte, value []byte) error {
	l.size += len(value)
	return l.lBatch.Put(key, value)
}

func (l *lsrvBatch) Delete(key []byte) error {
	l.size++
	return l.lBatch.Delete(key)
}

func (l *lsrvBatch) ValueSize() int {
	return l.size
}

func (l *lsrvBatch) Write() error {
	return l.lBatch.Write()
}

func (l *lsrvBatch) Reset() {
	l.size = 0
	l.lBatch = l.client.NewBatch()
}
