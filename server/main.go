// Copyright 2023 lucarondanini
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		// bitBox.Upsert("tasks", "key1", "one")
		// bitBox.Upsert("tasks", "key2", "two")
		// s := ""
		// bitBox.Get("tasks", "key1", &s)
		// fmt.Println(1, s)

		// it, e := bitBox.GetIterator("tasks")

		// if e != nil {
		// 	fmt.Println(e)
		// } else {
		// 	var k, v string
		// 	for it.HasMore() {
		// 		err := it.Next(&k, &v)
		// 		if err != nil {
		// 			fmt.Println(err)
		// 		} else {
		// 			fmt.Println(k, v)
		// 		}

		// 	}
		// }
		// <-onReadyChan

		bitBox.Start(forceRejoin, nil)
	}
}
