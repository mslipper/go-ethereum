package test

import (
	"testing"
	"context"
	"github.com/kyokan/levelsrv/pkg/server"
	"github.com/kyokan/levelsrv/pkg"
	"io/ioutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"fmt"
		"time"
	"github.com/kyokan/levelsrv/pkg/levelsrv"
)

func TestE2E(t *testing.T) {
	port := 50000
	ctx, cancel := context.WithCancel(context.Background())
	dbPath, err := ioutil.TempDir("", "levelsrc")
	require.NoError(t, err)
	err = server.Start(ctx, &pkg.Config{
		Port:   port,
		DBPath: dbPath,
	})
	require.NoError(t, err)

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
	require.NoError(t, err)
	client, err := levelsrv.NewClient(ctx, fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)

	key := []byte("test")
	value := []byte("value")

	err = client.Put(key, value)
	require.NoError(t, err)
	val, err := client.Get(key)
	require.NoError(t, err)
	require.Equal(t, string(val), "value")
	has, err := client.Has(key)
	require.NoError(t, err)
	require.True(t, has)
	has, err = client.Has([]byte("nope"))
	require.NoError(t, err)
	require.False(t, has)
	nilGet, err := client.Get([]byte("nope"))
	require.Error(t, err)
	require.Nil(t, nilGet)
	err = client.Delete(key)
	require.NoError(t, err)
	postDel, err := client.Has(key)
	require.NoError(t, err)
	require.False(t, postDel)

	err = client.Put([]byte("test2"), []byte("value2"))
	require.NoError(t, err)

	// test post-restart
	cancel()
	conn.Close()
	time.Sleep(1 * time.Second)
	port++
	ctx, cancel = context.WithCancel(context.Background())
	err = server.Start(ctx, &pkg.Config{
		Port:   port,
		DBPath: dbPath,
	})
	require.NoError(t, err)
	client, err = levelsrv.NewClient(ctx, fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	val, err = client.Get([]byte("test2"))
	require.NoError(t, err)
	require.Equal(t, string(val), "value2")
	cancel()
}
