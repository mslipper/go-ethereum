package ethdb

import (
	"context"
	"github.com/kyokan/levelsrv/pkg/storage"
	"github.com/kyokan/levelsrv/pkg/server"
	"github.com/kyokan/levelsrv/pkg"
	"time"
)

type LevelSRVDatabase struct {
	client storage.Store
	cancel context.CancelFunc
}

func NewLevelSRVDatabase(path string) (*LevelSRVDatabase, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lvlsrv, err := server.Start(ctx, &pkg.Config{
		DBPath: path,
		Port:   5900,
	})
	if err != nil {
		return nil, err
	}

	return &LevelSRVDatabase{
		client: lvlsrv,
		cancel: cancel,
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
	l.cancel()
	time.Sleep(3 * time.Second)
}

func (l *LevelSRVDatabase) NewBatch() Batch {
	return &lsrvBatch{
		client: l.client,
		lBatch: l.client.NewBatch(),
	}
}

type lsrvBatch struct {
	client storage.Store
	lBatch storage.Batch
	size   int
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
