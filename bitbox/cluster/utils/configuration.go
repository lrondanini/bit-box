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
