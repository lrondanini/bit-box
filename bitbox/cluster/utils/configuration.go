package utils

import (
	"fmt"
	"os"
)

type Configuration struct {
	LOGGER              Logger
	LOG_GOSSIP_PROTOCOL bool

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
