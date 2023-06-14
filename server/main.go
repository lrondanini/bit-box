package main

import (
	"flag"
	"os"

	"github.com/lrondanini/bit-box/bitbox"
	"github.com/lrondanini/bit-box/server/cli"
	cliUtils "github.com/lrondanini/bit-box/server/cli/utils"
)

var bitBox *bitbox.BitBox

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
		bb, err := bitbox.Init(cliUtils.GetConfigurationForCluster())
		if err != nil {
			panic(err)
		}

		bitBox = bb
	}
}

func main() {
	if cliMode {
		cli.Start(cliConfFile)
	} else {
		// onReadyChan := make(chan bool)
		// go bitBox.Start(forceRejoin, onReadyChan)
		// <-onReadyChan
		// // bitBox.Upsert("tasks", "key1", "one")
		// // bitBox.Upsert("tasks", "key2", "two")
		// // s := ""
		// // bitBox.Get("tasks", "key1", &s)
		// // fmt.Println(1, s)
		// it, e := bitBox.GetIterator("tasks")

		// if e != nil {
		// } else {
		// 	var k, v []byte
		// 	var hash uint64
		// 	for it.HasMore() {
		// 		hash, k, v, _ = it.NextRaw()
		// 		fmt.Println(hash, k, v)
		// 	}
		// }
		// <-onReadyChan

		bitBox.Start(forceRejoin, nil)
	}
}
