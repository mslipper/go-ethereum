package ethdb

import (
	"github.com/kyokan/levelsrv/pkg/levelsrv"
	"context"
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
		items:  make([]kv, 0),
	}
}

type lsrvBatch struct {
	client *levelsrv.Client
	size int
	items  []kv
}

func (l *lsrvBatch) Put(key []byte, value []byte) error {
	l.items = append(l.items, kv{
		k: key,
		v: value,
	})
	l.size += len(value)
	return nil
}

func (l *lsrvBatch) Delete(key []byte) error {
	l.items = append(l.items, kv{
		k:   key,
		del: true,
	})
	l.size++
	return nil
}

func (l *lsrvBatch) ValueSize() int {
	return l.size
}

func (l *lsrvBatch) Write() error {
	for _, entry := range l.items {
		var err error

		if entry.del {
			err = l.client.Delete(entry.k)
		} else {
			err = l.client.Put(entry.k, entry.v)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (l *lsrvBatch) Reset() {
	l.size = 0
	l.items = make([]kv, 0)
}
