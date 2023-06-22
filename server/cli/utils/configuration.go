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
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	CLI_IP            string `yaml:"ip"`
	CLI_PORT          string `yaml:"port"`
	CLUSTER_NODE_IP   string `yaml:"clusterNodeIp"`
	CLUSTER_NODE_PORT string `yaml:"clusterNodePort"`
}

var (
	configFileName = "bit-box-cli"
	configFileType = "yaml"
	configPaths    = []string{
		"/etc/bit-box/",
		"$HOME/.bit-box",
		".",
	}
)

func LoadConfiguration(confFilePath string) (Configuration, error) {
	if confFilePath != "" {
		fmt.Println("Using configuration file: ", confFilePath)
		locationPath := filepath.Dir(confFilePath)
		tmpFileName := filepath.Base(confFilePath)
		ext := filepath.Ext(confFilePath)
		if ext != ".yaml" && ext != ".yml" {
			panic("Configuration file requires .yaml or .yml extension")
		}
		fileName := strings.ReplaceAll(tmpFileName, ext, "")

		return initConfiguration(fileName, configFileType, []string{locationPath})
	} else {
		return initConfiguration(configFileName, configFileType, configPaths)
	}

}

func initConfiguration(fileName string, fileType string, paths []string) (Configuration, error) {
	viper.SetConfigName(fileName)
	viper.SetConfigType(fileType)

	// call multiple times to add many search paths
	for _, p := range configPaths {
		viper.AddConfigPath(p)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			conf := Configuration{}
			//trying to guess local ip and port
			if conf.CLI_IP == "" {
				conf.CLI_IP = findLocalIp()
			}

			if conf.CLI_PORT == "" {
				conf.CLI_PORT = findFreePort("", 0)
			}
			return conf, errors.New("CONFIG_FILE_NOT_FOUND")
		} else {
			// Config file was found but another error was produced
			panic(fmt.Errorf("error loading config: %w ", err))
		}
	}

	conf := Configuration{
		CLI_IP:            viper.GetString("ip"),
		CLI_PORT:          viper.GetString("port"),
		CLUSTER_NODE_IP:   viper.GetString("clusterNodeIp"),
		CLUSTER_NODE_PORT: viper.GetString("clusterNodePort"),
	}

	return conf, nil
}

func findLocalIp() string {
	netInterfaceAddresses, err := net.InterfaceAddrs()

	if err != nil {
		return ""
	}

	for _, netInterfaceAddress := range netInterfaceAddresses {
		networkIp, ok := netInterfaceAddress.(*net.IPNet)
		if ok && !networkIp.IP.IsLoopback() && networkIp.IP.To4() != nil {
			ip := networkIp.IP.String()
			return ip
		}
	}

	return ""
}

func findFreePort(p string, i int) string {
	port := p

	if port == "" {
		port = "36367"
	} else {
		tmp, _ := strconv.Atoi(port)
		tmp = tmp + 1
		port = strconv.Itoa(tmp)
		if i > 10 {
			return ""
		}
	}

	ln, err := net.Listen("tcp", ":"+port)

	if err != nil {
		//try next port
		return findFreePort(port, i+1)
	}

	ln.Close()
	return port
}

func getUserInput() string {
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	return strings.Replace(text, "\n", "", -1)
}

func getLocalIp(conf *Configuration) error {
	fmt.Print("Local IP [" + conf.CLI_IP + "]: ")
	cliIp := getUserInput()

	if cliIp == "" && conf.CLI_IP != "" {
		return nil
	} else if cliIp == "q" {
		return errors.New("USER_EXIT")
	} else if cliIp == "" && conf.CLI_IP == "" {
		fmt.Print("IP cannot be empty - type q to exit")
		return getLocalIp(conf)
	} else if cliIp != "" {
		if net.ParseIP(cliIp) == nil {
			fmt.Printf("Invalid IP Address: %s - type q to exit\n", cliIp)
			return getLocalIp(conf)
		}
		conf.CLI_IP = cliIp
	}

	return nil
}

func getLocalPort(conf *Configuration) error {
	fmt.Print("Local Port [" + conf.CLI_PORT + "]: ")
	cliPort := getUserInput()

	if cliPort == "" && conf.CLI_PORT != "" {
		return nil
	} else if cliPort == "q" {
		return errors.New("USER_EXIT")
	} else if cliPort == "" && conf.CLI_PORT == "" {
		fmt.Println("Port cannot be empty - type q to exit")
		fmt.Println()
		return getLocalIp(conf)
	} else if cliPort != "" {
		conf.CLI_PORT = cliPort
	}

	return nil
}

func getLocalNetConf(conf *Configuration) error {
	err := getLocalIp(conf)
	if err != nil {
		return err
	}

	err = getLocalPort(conf)
	if err != nil {
		return err
	}

	//test configuration:
	ln, err := net.Listen("tcp", conf.CLI_IP+":"+conf.CLI_PORT)

	if err != nil {
		//try next port
		fmt.Println("Error: " + err.Error())
		fmt.Println()
		getLocalNetConf(conf)
	} else {
		ln.Close()
	}

	return nil
}

func getRemoteIp(conf *Configuration) error {
	fmt.Print("Cluster Node IP [" + conf.CLUSTER_NODE_IP + "]: ")
	cliIp := getUserInput()

	if cliIp == "" && conf.CLUSTER_NODE_IP != "" {
		return nil
	} else if cliIp == "q" {
		return errors.New("USER_EXIT")
	} else if cliIp == "" && conf.CLUSTER_NODE_IP == "" {
		fmt.Println("IP cannot be empty - type q to exit")
		fmt.Println()
		return getRemoteIp(conf)
	} else if cliIp != "" {
		if cliIp != "localhost" {
			if net.ParseIP(cliIp) == nil {
				fmt.Printf("Invalid IP Address: %s - type q to exit\n", cliIp)
				fmt.Println()
				return getRemoteIp(conf)
			}
		}
		conf.CLUSTER_NODE_IP = cliIp
	}

	return nil
}

func getRemotePort(conf *Configuration) error {
	fmt.Print("Cluster Node Port [" + conf.CLUSTER_NODE_PORT + "]: ")
	cliPort := getUserInput()

	if cliPort == "" && conf.CLUSTER_NODE_PORT != "" {
		return nil
	} else if cliPort == "q" {
		return errors.New("USER_EXIT")
	} else if cliPort == "" && conf.CLUSTER_NODE_PORT == "" {
		fmt.Print("Port cannot be empty - type q to exit")
		fmt.Println()
		return getRemotePort(conf)
	} else if cliPort != "" {
		conf.CLUSTER_NODE_PORT = cliPort
	}

	return nil
}

func getRemoteNetConf(conf *Configuration) error {
	err := getRemoteIp(conf)
	if err != nil {
		return err
	}

	err = getRemotePort(conf)
	if err != nil {
		return err
	}

	return nil
}

func ConfigureCli(conf *Configuration) error {
	err := getLocalNetConf(conf)
	if err != nil {
		return err
	}

	err = getRemoteNetConf(conf)
	if err != nil {
		return err
	}

	confFileName := "./" + configFileName + ".yml"
	fmt.Print("Saving conf to " + confFileName + "? (Y/n): ")
	goSave := getUserInput()
	if goSave == "" || goSave == "y" || goSave == "Y" {
		yamlData, err := yaml.Marshal(&conf)
		if err != nil {
			fmt.Println("Error: " + err.Error())
		} else {
			yamlData = append([]byte("# Conf file for bit-box CLI\n\n"), yamlData...)
			err := os.WriteFile(confFileName, yamlData, 0644)
			if err != nil {
				fmt.Println("Could not save conf file: " + err.Error())
			}
		}
	}
	fmt.Println()

	return nil
}
