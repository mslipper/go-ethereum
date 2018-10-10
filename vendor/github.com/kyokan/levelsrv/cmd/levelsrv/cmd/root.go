package cmd

import (
	"github.com/spf13/cobra"
	"fmt"
	"os"
)

var rootCmd = &cobra.Command{
	Use: "levelsrv",
	Short: "A gRPC wrapper for leveldb key/value stores",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
	    fmt.Println(err)
	    os.Exit(1)
	}
}