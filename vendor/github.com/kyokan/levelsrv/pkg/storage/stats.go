package storage

import (
	log "github.com/inconshreveable/log15"
	"sync/atomic"
	"github.com/kyokan/levelsrv/pkg"
	"time"
)

type Stats struct {
	log          log.Logger
	writes       uint64
	reads        uint64
	bytesWritten uint64
	bytesRead    uint64
	quitChan     chan bool
}

func NewStats() *Stats {
	s := &Stats{
		log:      pkg.NewLogger("stats"),
		quitChan: make(chan bool),
	}

	go func() {
		tick := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-tick.C:
				s.logStats()
			case <-s.quitChan:
				return
			}
		}
	}()

	return s
}

func (s *Stats) RecordWrite() {
	atomic.AddUint64(&s.writes, 1)
}

func (s *Stats) RecordRead() {
	atomic.AddUint64(&s.reads, 1)
}

func (s *Stats) RecordBytesWritten(size uint64) {
	atomic.AddUint64(&s.bytesWritten, size)
}

func (s *Stats) RecordBytesRead(size uint64) {
	atomic.AddUint64(&s.bytesRead, size)
}

func (s *Stats) Stop() {
	s.quitChan <- true
}

func (s *Stats) logStats() {
	s.log.Info(
		"db stats",
		"reads",
		atomic.LoadUint64(&s.reads),
		"writes",
		atomic.LoadUint64(&s.writes),
		"bytes_written",
		atomic.LoadUint64(&s.bytesWritten),
		"bytes_read",
		atomic.LoadUint64(&s.bytesRead),
	)

	atomic.StoreUint64(&s.reads, 0)
	atomic.StoreUint64(&s.writes, 0)
	atomic.StoreUint64(&s.bytesWritten, 0)
	atomic.StoreUint64(&s.bytesRead, 0)
}
