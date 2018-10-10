package server

import (
	"github.com/kyokan/levelsrv/pkg"
	"github.com/kyokan/levelsrv/pkg/storage"
	"net"
	"fmt"
	"github.com/kyokan/levelsrv/pkg/pb"
	"google.golang.org/grpc"
	"context"
)

func Start(ctx context.Context, cfg *pkg.Config) error {
	log := pkg.NewLogger("server")
	store, err := storage.NewLevelDBStore(cfg.DBPath)
	if err != nil {
		return err
	}

	srv := &NodeServer{
		store: store,
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return err
	}

	gSrv := grpc.NewServer()
	node.RegisterNodeServer(gSrv, srv)
	errCh := make(chan error)
	go func() {
		if err := gSrv.Serve(lis); err != nil {
		    errCh <- err
		}

		return
	}()

	go func() {
		for {
			select {
			case err := <-errCh:
				log.Error("caught error", "err", err)
				return
			case <-ctx.Done():
				log.Info("shutting down")
				gSrv.Stop()
				store.Close()
				log.Info("goodbye")
				return
			}
		}
	}()

	log.Info("server started", "port", cfg.Port)

	return nil
}
