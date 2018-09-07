package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
	log "github.com/inconshreveable/log15"
	"github.com/kyokan/levelsrv/pkg"
)

type LevelDBStore struct {
	db  *leveldb.DB
	log log.Logger
	stats *Stats
}

func NewLevelDBStore(path string) (Store, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	logger := pkg.NewLogger("leveldbstore")
	logger.Info("initialized leveldb store")

	return &LevelDBStore{
		db:  db,
		log: logger,
		stats: NewStats("leveldb"),
	}, nil
}

func (l *LevelDBStore) Put(key []byte, value []byte) error {
	l.stats.RecordBytesRead(uint64(len(value)))
	l.stats.RecordWrite()
	return l.db.Put(key, value, nil)
}

func (l *LevelDBStore) Delete(key []byte) error {
	return l.db.Delete(key, nil)
}

func (l *LevelDBStore) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, nil)
}

func (l *LevelDBStore) Has(key []byte) (bool, error) {
	return l.db.Has(key, nil)
}

func (l *LevelDBStore) Close() {
	if err := l.db.Close(); err != nil {
		l.log.Error("failed to close database", "err", err)
		return
	}

	l.stats.Stop()
	l.log.Info("closed leveldb store")
}

func (l *LevelDBStore) NewBatch() Batch {
	return newLdbBatch(l.db)
}

type ldbBatch struct {
	ldb   *leveldb.DB
	batch *leveldb.Batch
}

func newLdbBatch(ldb *leveldb.DB) Batch {
	return &ldbBatch{
		ldb:   ldb,
		batch: new(leveldb.Batch),
	}
}

func (b *ldbBatch) Put(key []byte, value []byte) error {
	b.batch.Put(key, value)
	return nil
}

func (b *ldbBatch) Delete(key []byte) error {
	b.batch.Delete(key)
	return nil
}

func (b *ldbBatch) Write() error {
	return b.ldb.Write(b.batch, nil)
}

func (b *ldbBatch) Reset() {
	b.batch.Reset()
}
