package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/lrondanini/bit-box/server/cli"
	cliUtils "github.com/lrondanini/bit-box/server/cli/utils"

	"github.com/lrondanini/bit-box/bitbox/cluster"
)

var node *cluster.Node

var cliMode = false
var cliConfFile = ""
var forceRejoin = false

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func parseCommandLineParameters() (bool, string, bool) {
	flag.Bool("h", false, "Shows this help")
	useConfFilePtr := flag.String("c", "", "Specify configuration file")
	portPtr := flag.String("p", "", "Specify server's listening port")
	flag.Bool("cli", false, "Start cli for cluster management")
	flag.Bool("rejoin", false, "With this option you can re-join a cluster after a node decommission")

	flag.Parse()

	if isFlagPassed("cli") {
		cliConfFilePath := ""
		if isFlagPassed("c") {
			cliConfFilePath = *useConfFilePtr
		}
		return true, cliConfFilePath, false
	}

	if isFlagPassed("h") {
		flag.Usage()
		os.Exit(0)
	} else if isFlagPassed("c") {
		cliUtils.SetConfFile(*useConfFilePtr)
	}

	if isFlagPassed("p") {
		cliUtils.OverwritePort(*portPtr)
	}

	forceRejoin := false
	if isFlagPassed("rejoin") {
		forceRejoin = true
	}

	// cliMode, cliConfFile, forceRejoin
	return false, "", forceRejoin
}

func init() {
	cliMode, cliConfFile, forceRejoin = parseCommandLineParameters()

	if !cliMode {
		n, err := cluster.InitNode(*cliUtils.GetClusterConfiguration())
		if err != nil {
			panic(err)
		}

		node = n
	}
}

func main() {
	if cliMode {
		cli.Start(cliConfFile)
	} else {

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

		if err := run(ctx, signalChan); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}
}

func run(ctx context.Context, signalChan chan os.Signal) error {
	return node.Start(ctx, signalChan, forceRejoin)
}
