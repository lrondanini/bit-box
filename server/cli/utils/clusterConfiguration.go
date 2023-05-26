package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/spf13/viper"
)

var instance utils.Configuration
var once sync.Once

var (
	clusterConfigFileName = "config"
	clusterConfigFileType = "yaml"
	clusterConfigPaths    = []string{
		"/etc/bit-box/",
		"$HOME/.bit-box",
		".",
	}
)

func GetClusterConfiguration() *utils.Configuration {
	once.Do(func() {
		instance = initClusterConfiguration(clusterConfigFileName, clusterConfigFileType, clusterConfigPaths)
	})
	return &instance
}

func SetConfFile(confFilePath string) {
	fmt.Println("Using configuration file: ", confFilePath)
	locationPath := filepath.Dir(confFilePath)
	tmpFileName := filepath.Base(confFilePath)
	ext := filepath.Ext(confFilePath)
	if ext != ".yaml" && ext != ".yml" {
		panic("Configuration file requires .yaml or .yml extension")
	}

	fileName := strings.ReplaceAll(tmpFileName, ext, "")

	if !filepath.IsAbs(confFilePath) {
		locationPath = filepath.Join(os.Getenv("PWD"), locationPath)
	}

	once.Do(func() {
		instance = initClusterConfiguration(fileName, clusterConfigFileType, []string{locationPath})
	})
}

// warning: if calling SetConfFile after OverwritePort, the conf file wont be loaded
func OverwritePort(port string) {
	once.Do(func() {
		instance = initClusterConfiguration(clusterConfigFileName, clusterConfigFileType, clusterConfigPaths)
	})
	instance.NODE_PORT = port
}

func initClusterConfiguration(fileName string, fileType string, paths []string) utils.Configuration {

	viper.SetConfigName(fileName)
	viper.SetConfigType(fileType)

	// call multiple times to add many search paths
	for _, p := range paths {
		viper.AddConfigPath(p)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			panic(fmt.Errorf("fatal error config file: %w ", err))
		} else {
			// Config file was found but another error was produced
			panic(fmt.Errorf("error loading config: %w ", err))
		}
	}

	conf := utils.Configuration{
		LOG_LEVEL:                  viper.GetString("logLevel"),
		LOG_TO:                     viper.GetString("logTo"),
		LOG_DIR:                    viper.GetString("logDir"),
		LOG_FILE_NAME:              viper.GetString("logFileName"),
		LOG_FILE_MAX_SIZE:          viper.GetInt("logMaxSize"),
		LOG_FILE_MAX_NUM_BACKUPS:   viper.GetInt("logMaxBackups"),
		LOG_FILE_MAX_AGE:           viper.GetInt("logMaxAge"),
		LOG_GOSSIP_PROTOCOL:        viper.GetBool("logGossipProtocol"),
		NODE_IP:                    viper.GetString("nodeIp"),
		NODE_PORT:                  viper.GetString("nodePort"),
		NODE_HEARTBIT_PORT:         viper.GetString("nodeHeartbitPort"),
		CLUSTER_NODE_IP:            viper.GetString("clusterNodeIp"),
		CLUSTER_NODE_PORT:          viper.GetString("clusterNodePort"),
		CLUSTER_NODE_HEARTBIT_PORT: viper.GetString("clusterNodeHeartbitPort"),
		DATA_FOLDER:                viper.GetString("dataFolder"),
	}

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

	if len(errors) > 0 {
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "%s\n", e)
		}
		panic("Configuration errors")
	}

	return conf
}
