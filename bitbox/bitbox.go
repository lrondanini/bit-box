package bitbox

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/lrondanini/bit-box/bitbox/cluster"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/storage"
)

type BitBox struct {
	node *cluster.Node
}

func Init(configuration utils.Configuration) (*BitBox, error) {
	n, err := cluster.InitNode(configuration)
	if err != nil {
		return nil, err
	}

	return &BitBox{
		node: n,
	}, nil
}

func (bb *BitBox) Start(forceRejoin bool, onReadyChan chan bool) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	go func() {
		select {
		case <-signalChan: // first signal, cancel context
			fmt.Println("\n\nGracefully shutting down")
			cancel()
		case <-ctx.Done():
		}
		<-signalChan // second signal, hard exit
		os.Exit(2)
	}()

	if err := bb.run(ctx, signalChan, forceRejoin, onReadyChan); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func (bb *BitBox) run(ctx context.Context, signalChan chan os.Signal, forceRejoin bool, onReadyChan chan bool) error {
	return bb.node.Start(ctx, signalChan, forceRejoin, onReadyChan)
}

func (bb *BitBox) Upsert(collectionName string, key interface{}, value interface{}) error {
	return bb.node.Upsert(collectionName, key, value)
}

func (bb *BitBox) Get(collectionName string, key interface{}, value interface{}) error {
	return bb.node.Get(collectionName, key, value)
}

func (bb *BitBox) Delete(collectionName string, key interface{}) error {
	return bb.node.Delete(collectionName, key)
}

func (bb *BitBox) GetIterator(collectionName string) (*storage.Iterator, error) {
	return bb.node.GetIterator(collectionName)
}

func (bb *BitBox) GetIteratorFrom(collectionName string, from interface{}) (*storage.Iterator, error) {
	return bb.node.GetIteratorFrom(collectionName, from)
}

func (bb *BitBox) GetFilteredIterator(collectionName string, from interface{}, to interface{}) (*storage.Iterator, error) {
	return bb.node.GetFilteredIterator(collectionName, from, to)
}

func (bb *BitBox) DeleteCollection(collectionName string) error {
	return bb.node.DeleteCollection(collectionName)
}
