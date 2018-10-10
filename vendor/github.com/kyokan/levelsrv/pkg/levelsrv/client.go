package levelsrv

import (
	"context"
	"github.com/kyokan/levelsrv/pkg/pb"
	"google.golang.org/grpc"
)

type Client struct {
	client node.NodeClient
	ctx    context.Context
	conn   *grpc.ClientConn
}

func NewClient(ctx context.Context, url string) (*Client, error) {
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := node.NewNodeClient(conn)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	return &Client{
		client: client,
		ctx:    ctx,
		conn:   conn,
	}, nil
}

func (c *Client) Put(key []byte, value []byte) error {
	_, err := c.client.Put(context.TODO(), &node.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

func (c *Client) Delete(key []byte) error {
	_, err := c.client.Delete(context.TODO(), &node.DeleteRequest{
		Key: key,
	})
	return err
}

func (c *Client) Get(key []byte) ([]byte, error) {
	val, err := c.client.Get(context.TODO(), &node.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	return val.Value, nil
}

func (c *Client) Has(key []byte) (bool, error) {
	val, err := c.client.Has(context.TODO(), &node.HasRequest{
		Key: key,
	})
	if err != nil {
		return false, err
	}
	return val.Value, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
