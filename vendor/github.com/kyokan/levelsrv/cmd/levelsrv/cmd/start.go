package cmd

import (
	"github.com/spf13/cobra"
	"github.com/kyokan/levelsrv/pkg"
	"github.com/kyokan/levelsrv/pkg/server"
	"context"
	"time"
)

var dbPath string
var port int

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "starts levelsrv",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := &pkg.Config{
			DBPath: dbPath,
			Port:   port,
		}

		ctx, cancel := context.WithCancel(context.Background())

		err := server.Start(ctx, cfg)
		if err != nil {
			cancel()
			return err
		}

		pkg.AwaitTermination(func() {
			cancel()
			time.Sleep(1 * time.Second)
		})

		return nil
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	startCmd.Flags().StringVar(&dbPath, "db-path", "", "path to database")
	startCmd.Flags().IntVarP(&port, "port", "", 5900, "port to listen on")
	startCmd.MarkFlagRequired("db-path")
}
