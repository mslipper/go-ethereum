package pkg

import log "github.com/inconshreveable/log15"

func NewLogger(module string) log.Logger {
	return log.New("module", module)
}