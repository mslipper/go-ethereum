package storage

import (
	"sync"
	"errors"
)

type MemStore struct {
	data map[string][]byte
	mtx sync.RWMutex
}

func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string][]byte),
	}
}

func (m *MemStore) Put(key []byte, value []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.data[string(key)] = value
	return nil
}

func (m *MemStore) Delete(key []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.data, string(key))
	return nil
}

func (m *MemStore) Get(key []byte) ([]byte, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	val, has := m.data[string(key)]
	if !has {
		return nil, errors.New("not found")
	}
	return val, nil
}

func (m *MemStore) Has(key []byte) (bool, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	_, has := m.data[string(key)]
	return has, nil
}

func (m *MemStore) Close() {
	// noop
}

func (m *MemStore) NewBatch() Batch {
	return NewMembatch(m.writeBatch)
}

func (m *MemStore) writeBatch(kvs []*KV) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, kv := range kvs {
		if kv.IsDel {
			delete(m.data, string(kv.K))
		} else {
			m.data[string(kv.K)] = kv.V
		}
	}

	return nil
}
