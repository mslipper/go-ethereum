package pkg

import (
	"os"
	"os/signal"
	"syscall"
)

func AwaitTermination(onSignal func()) {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<- sigs
		done <- true
	}()

	<- done
	onSignal()
}