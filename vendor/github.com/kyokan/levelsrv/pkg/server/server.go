package server

import (
	"github.com/kyokan/levelsrv/pkg/pb"
	"golang.org/x/net/context"
	"github.com/kyokan/levelsrv/pkg/storage"
)

type NodeServer struct {
	store storage.Store
}

func (n *NodeServer) Put(ctx context.Context, req *node.PutRequest) (*node.PutResponse, error) {
	err := n.store.Put(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &node.PutResponse{}, err
}

func (n *NodeServer) Get(ctx context.Context, req *node.GetRequest) (*node.GetResponse, error) {
	value, err := n.store.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &node.GetResponse{
		Value: value,
	}, nil
}

func (n *NodeServer) Has(ctx context.Context, req *node.HasRequest) (*node.HasResponse, error) {
	has, err := n.store.Has(req.Key)
	if err != nil {
		return nil, err
	}
	return &node.HasResponse{Value: has}, nil
}

func (n *NodeServer) Delete(ctx context.Context, req *node.DeleteRequest) (*node.DeleteResponse, error) {
	err := n.store.Delete(req.Key)
	if err != nil {
		return nil, err
	}
	return &node.DeleteResponse{}, nil
}
