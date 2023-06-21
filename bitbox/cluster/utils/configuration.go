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

package utils

import (
	"fmt"
	"os"
)

type Configuration struct {
	LOGGER              Logger
	LOG_GOSSIP_PROTOCOL bool
	LOG_STORAGE         bool

	NODE_IP            string
	NODE_PORT          string
	NODE_HEARTBIT_PORT string

	CLUSTER_NODE_IP            string
	CLUSTER_NODE_PORT          string
	CLUSTER_NODE_HEARTBIT_PORT string

	NUMB_VNODES int

	DATA_FOLDER string
}

var confInstance Configuration

func GetClusterConfiguration() *Configuration {
	return &confInstance
}

func VerifyAndSetConfiguration(conf *Configuration) {
	//validate configuration
	var errors []string

	if conf.NODE_IP == "" {
		errors = append(errors, "nodeIp is required")
	}

	if conf.NODE_IP == "" {
		errors = append(errors, "nodeIp is required")
	}

	if conf.NODE_HEARTBIT_PORT == "" {
		errors = append(errors, "nodeHeartbitPort is required")
	}

	if conf.NODE_PORT == "" {
		errors = append(errors, "nodePort is required")
	}

	if conf.DATA_FOLDER == "" {
		errors = append(errors, "dataFolder is required")
	}

	if conf.NUMB_VNODES == 0 {
		conf.NUMB_VNODES = 8
	}

	InitLogger(conf.LOGGER)

	if len(errors) > 0 {
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "%s\n", e)
		}
		panic("Configuration errors")
	}

	confInstance = *conf
}

func GetConfForTesting() *Configuration {
	return &Configuration{
		LOGGER:              &LoggerForTesting{},
		LOG_GOSSIP_PROTOCOL: false,
		LOG_STORAGE:         false,

		NODE_IP:            "localhost",
		NODE_PORT:          "9999",
		NODE_HEARTBIT_PORT: "9998",

		CLUSTER_NODE_IP:            "localhost",
		CLUSTER_NODE_PORT:          "9997",
		CLUSTER_NODE_HEARTBIT_PORT: "9996",

		NUMB_VNODES: 4,

		DATA_FOLDER: "/Users/lucarondanini/Desktop/BitBox/data/node-tests",
	}
}
