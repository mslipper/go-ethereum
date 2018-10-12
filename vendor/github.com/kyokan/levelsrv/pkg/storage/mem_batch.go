package storage

type BatchWriter = func([]KV) error

type Membatch struct {
	items  []KV
	writer BatchWriter
}

type KV struct {
	K     []byte
	V     []byte
	IsDel bool
}

func NewMembatch(writer BatchWriter) *Membatch {
	return &Membatch{
		writer: writer,
	}
}

func (m *Membatch) Put(key []byte, value []byte) error {
	m.items = append(m.items, KV{
		K: key,
		V: value,
	})
	return nil
}

func (m *Membatch) Delete(key []byte) error {
	m.items = append(m.items, KV{
		K:     key,
		IsDel: true,
	})
	return nil
}

func (m *Membatch) Write() error {
	return m.writer(m.items)
}

func (m *Membatch) Reset() {
	m.items = make([]KV, 0)
}
