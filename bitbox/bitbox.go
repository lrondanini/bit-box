package bitbox

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/lrondanini/bit-box/bitbox/cluster"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
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

func (bb *BitBox) Start(forceRejoin bool) {
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

	if err := bb.run(ctx, signalChan, forceRejoin); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func (bb *BitBox) run(ctx context.Context, signalChan chan os.Signal, forceRejoin bool) error {
	return bb.node.Start(ctx, signalChan, forceRejoin)
}
