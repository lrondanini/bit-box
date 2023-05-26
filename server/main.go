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
		bb, err := bitbox.Init(*cliUtils.GetClusterConfiguration())
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
		bitBox.Start(forceRejoin)
	}
}
