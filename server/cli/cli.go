package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"

	cliUtils "github.com/lrondanini/bit-box/server/cli/utils"

	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/communication/types"

	"github.com/common-nighthawk/go-figure"
	"github.com/eiannone/keyboard"
	"github.com/manifoldco/promptui"

	//reminder, table needs import like: "github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/table"
)

func Start(confFilePath string) {
	cli := initCLI(confFilePath)
	cli.Run()
}

//******************** CLI ********************

const HISTORY_FILE_NAME = ".bit-box-cli-history"

type userInput struct {
	cmd    string
	params []string
}

type CLI struct {
	conf       *cliUtils.Configuration
	cluster    *cliUtils.Cluster
	cmdHistory []string
	mu         sync.Mutex
}

func initCLI(confFilePath string) *CLI {
	fmt.Println()
	myFigure := figure.NewFigure("Bit-Box", "graffiti", true)
	myFigure.Print()
	fmt.Println()
	fmt.Println()

	conf, err := cliUtils.LoadConfiguration(confFilePath)
	missingConfFile := false
	if err != nil {
		if err.Error() == "CONFIG_FILE_NOT_FOUND" {
			missingConfFile = true
		} else {
			panic(err)
		}
	}

	cluster := &cliUtils.Cluster{}

	if missingConfFile {
		fmt.Println("No configuration file found, please configure the cli connection to the cluster:")
		err = cliUtils.ConfigureCli(&conf)
		if err == nil {
			cluster = cliUtils.InitCluster(conf.CLI_IP+":"+conf.CLI_PORT, conf.CLI_IP, conf.CLI_PORT)
		}
	} else {
		if conf.CLI_IP != "" && conf.CLI_PORT != "" {
			cluster = cliUtils.InitCluster(conf.CLI_IP+":"+conf.CLI_PORT, conf.CLI_IP, conf.CLI_PORT)
		}
	}

	//LOAD HISTORY
	cmdHistory := []string{}
	cmdHistory = append(cmdHistory, "")

	_, err = os.Stat(HISTORY_FILE_NAME)

	if os.IsNotExist(err) {
		file, err := os.Create(HISTORY_FILE_NAME)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		defer file.Close()
	} else {
		file, err := os.Open(HISTORY_FILE_NAME)
		if err != nil {
			fmt.Println("Possible solution: delete file " + HISTORY_FILE_NAME)
			panic(err)
		}
		defer file.Close()
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("Possible solution: delete file .bit-box-cli-history" + HISTORY_FILE_NAME)
				panic(err)
			}
			cmdHistory = append(cmdHistory, strings.ReplaceAll(line, "\n", ""))
		}
	}
	//-------------------

	return &CLI{
		conf:       &conf,
		cluster:    cluster,
		cmdHistory: cmdHistory,
	}
}

func (cli *CLI) Run() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	defer func() {
		signal.Stop(signalChan)
	}()

	go func() {
		<-signalChan // detect exit
		cli.Shutdown()
		fmt.Println()
		fmt.Println("...cya!")
		os.Exit(0)
	}()

	if cli.cluster.NodeId != "" {
		cli.cluster.StartListening()
		if cli.conf.CLUSTER_NODE_IP != "" && cli.conf.CLUSTER_NODE_PORT != "" {
			cli.PingClusterNode()
		}
	}

	fmt.Println()
	fmt.Println("Welcome to bit-box cli, press Enter for a list of commands")

	exiting := false
	for {
		if !exiting {
			uInput := cli.waitForUserInputInput()
			switch uInput.cmd {
			case "help", "h":
				cli.PrintHelp()
			case "exit", "e", "q", "quit":
				exiting = true
				signalChan <- os.Interrupt
			case "conf-cli", "c":
				if cli.cluster.NodeId != "" {
					cli.cluster.Shutdown()
				}
				err := cliUtils.ConfigureCli(cli.conf)
				if err == nil {
					//restart cluster:
					cli.cluster = cliUtils.InitCluster(cli.conf.CLI_IP+":"+cli.conf.CLI_PORT, cli.conf.CLI_IP, cli.conf.CLI_PORT)
					cli.cluster.StartListening()
				}
			case "ping", "p":
				cli.PingClusterNode()
			case "partition-table", "pt":
				cli.PrintPartitionTable()
			case "remove-node", "rn":
				if len(uInput.params) > 0 {
					cli.RemoveNode(uInput.params[0])
				} else {
					cli.RemoveNode("")
				}
			case "status", "s":
				cli.PrintClusterStatus()
			case "node-stats", "ns":
				if len(uInput.params) > 0 {
					cli.GetNodeStats(uInput.params[0])
				} else {
					cli.GetNodeStats("")
				}
			case "sync-status", "ss":
				if len(uInput.params) > 0 {
					cli.GetNodeSyncStatus(uInput.params[0])
				} else {
					cli.GetNodeSyncStatus("")
				}
			case "retry-sync-task", "rst":
				if len(uInput.params) == 1 {
					cli.RetrySyncTask(uInput.params[0])
				} else {
					fmt.Println("Wrong number of parameters, requires task-id")
				}

			case "set":
				if len(uInput.params) == 3 {
					cli.PerformSet(uInput.params[0], uInput.params[1], uInput.params[2])
				} else {
					fmt.Println("Wrong number of parameters")
				}
			case "get":
				if len(uInput.params) == 2 {
					cli.PerformGet(uInput.params[0], uInput.params[1])
				} else {
					fmt.Println("Wrong number of parameters")
				}
			case "del":
				if len(uInput.params) == 2 {
					cli.PerformDel(uInput.params[0], uInput.params[1])
				} else {
					fmt.Println("Wrong number of parameters")
				}
			case "scan":
				if len(uInput.params) >= 1 {
					if len(uInput.params) == 1 {
						cli.PerformScan(uInput.params[0], "", 0, false, "")
					} else if len(uInput.params) == 2 {
						cli.PerformScan(uInput.params[0], uInput.params[1], 0, false, "")
					} else if len(uInput.params) == 3 {
						n, e := strconv.Atoi(uInput.params[2])
						if e != nil {
							fmt.Println(e)
						} else {
							cli.PerformScan(uInput.params[0], uInput.params[1], n, false, "")
						}
					}
				} else {
					fmt.Println("Wrong number of parameters")
				}
			case "locate", "loc":
				if len(uInput.params) == 1 {
					cli.PerformLocate(uInput.params[0])
				} else {
					fmt.Println("Wrong number of parameters")
				}
			case "test":
				cli.PerformSet("dogs", "pluto", "one")
				cli.PerformSet("dogs", "pippo", "two")
				cli.PerformSet("dogs", "paperino", "three")
			default:
				if uInput.cmd != "" {
					fmt.Println("Unknown command: " + uInput.cmd)
				} else {
					cli.PrintHelp()
				}
			}
		}
	}
}

func (cli *CLI) PrintHelp() {
	fmt.Println()
	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	fmt.Fprintln(writer, "(short)\tCOMMAND\tPARAMETERS\tDESCRIPTION")
	fmt.Fprintln(writer, "\t\t\t")
	fmt.Fprintln(writer, "(h)\thelp\t\tShow this help")
	fmt.Fprintln(writer, "(e,q)\texit, quit\t\tClose the cli")
	fmt.Fprintln(writer, "(p)\tping\t\tVerify connection with cluster node")
	fmt.Fprintln(writer, "(c)\tconf-cli\t\tView and/or configure the cli connection to the cluster")
	fmt.Fprintln(writer, "\t\t\t")
	fmt.Fprintln(writer, "(s)\tstatus\t\tShow clusters status")
	fmt.Fprintln(writer, "(nl)\tnodes-list\t\tLists all the nodes in the cluster")
	fmt.Fprintln(writer, "(ns)\tnode-stats\t[node-id]\tReturns stats for a specific node, if node-id is empty will prompt a list of nodes to choose from")
	fmt.Fprintln(writer, "(rn)\tremove-node\t[node-id]\tDecommissions a node from the cluster, if node-id is empty will prompt a list of nodes to choose from")
	fmt.Fprintln(writer, "(pt)\tpartition-table\t\tShow tokens distribution among nodes")
	fmt.Fprintln(writer, "(ss)\tsync-status\t[node-id]\tShow sync status for node if node-id is empty will prompt a list of nodes to choose from")
	fmt.Fprintln(writer, "(rst)\tretry-sync-task\ttask-id\tRetry sync task with task-id (helpful when a node is stuck in sync for crashes or net partitions)")
	fmt.Fprintln(writer, "\t\t\t")
	fmt.Fprintln(writer, "\tset\tcollection key value\tSet key to value for collection")
	fmt.Fprintln(writer, "\tget\tcollection key\tGet key value from collection")
	fmt.Fprintln(writer, "\tdel\tcollection key\tDelete key from collection")
	fmt.Fprintln(writer, "\tscan\tcollection [key] [size]\tScan node's content starting from key and retrieving size results per fetch")
	fmt.Fprintln(writer, "(loc)\tlocate\tkey\tReturns information about the node that owns the key")
	writer.Flush()
	fmt.Println()
}

func (cli *CLI) PingClusterNode() {
	nodeId, err := cli.GetClusterNodeId()

	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = cli.cluster.CommManager.SendPing(nodeId)

	if err != nil {
		fmt.Println("Could not connect to cluster node: " + err.Error())
	} else {
		fmt.Println("Connected to cluster:" + cli.conf.CLUSTER_NODE_IP + ":" + cli.conf.CLUSTER_NODE_PORT)
	}
}

func (cli *CLI) GetClusterNodeId() (string, error) {
	if cli.conf.CLUSTER_NODE_IP != "" && cli.conf.CLUSTER_NODE_PORT != "" {
		return cli.conf.CLUSTER_NODE_IP + ":" + cli.conf.CLUSTER_NODE_PORT, nil
	}
	return "", errors.New("cluster node not configured")
}

func (cli *CLI) Shutdown() {
	if cli.cluster.NodeId != "" {
		cli.cluster.Shutdown()
	}
}

func (cli *CLI) waitForUserInputInput() userInput {
	if err := keyboard.Open(); err != nil {
		panic(err)
	}
	defer func() {
		_ = keyboard.Close()
	}()

	head := "bit-box> "

	input := ""

	historyPointer := len(cli.cmdHistory)

	fmt.Print(head)
LOOP:
	for {
		char, key, err := keyboard.GetKey()
		if err != nil {
			panic(err)
		}

		switch key {
		case keyboard.KeyArrowDown:
			historyPointer++
			if historyPointer >= len(cli.cmdHistory) {
				historyPointer = len(cli.cmdHistory) - 1
			}
			input = cli.cmdHistory[historyPointer]
			fmt.Printf("\033[2K\r")
			fmt.Print(head + input)
		case keyboard.KeyArrowUp:
			historyPointer--
			if historyPointer < 0 {
				historyPointer = 0
			}
			input = cli.cmdHistory[historyPointer]
			fmt.Printf("\033[2K\r")
			fmt.Print(head + input)
		case keyboard.KeyEsc:
			break LOOP
		case keyboard.KeyEnter:
			break LOOP
		case keyboard.KeySpace:
			input += " "
			fmt.Printf("\033[2K\r")
			fmt.Print(head + input)
		case keyboard.KeyBackspace, keyboard.KeyBackspace2:
			if len(input) > 0 {
				input = input[:len(input)-1]
				fmt.Printf("\033[2K\r")
				fmt.Print(head + input)
			}
		case keyboard.KeyCtrlC:
			input = "quit"
			break LOOP
		default:
			input += string(char)
			fmt.Printf("\033[2K\r")
			fmt.Print(head + input)
		}
	}

	trim := regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

	tmp := strings.Split(input, " ")
	uInput := userInput{
		cmd:    trim.ReplaceAllString(tmp[0], ""),
		params: tmp[1:],
	}

	if uInput.cmd != "" && uInput.cmd != "quit" && uInput.cmd != "exit" && uInput.cmd != "q" && uInput.cmd != "e" {
		if cli.cmdHistory[len(cli.cmdHistory)-1] != input {
			//push only if it's not the same as the last one
			cli.mu.Lock()
			cli.cmdHistory = append(cli.cmdHistory, input)
			cli.mu.Unlock()
		}

		if len(cli.cmdHistory) > 100 {
			//keep the latest 100 elements
			cli.mu.Lock()
			cli.cmdHistory = cli.cmdHistory[len(cli.cmdHistory)-100:]
			cli.mu.Unlock()
		}

		go cli.saveCmdHistory()
	}

	fmt.Println()
	return uInput
}

func (cli *CLI) saveCmdHistory() {
	str := ""
	cli.mu.Lock()
	for _, v := range cli.cmdHistory {
		str += v + "\n"
	}
	cli.mu.Unlock()
	err := os.WriteFile(HISTORY_FILE_NAME, []byte(str), 0644)
	if err != nil {
		fmt.Println("Could not save cmd history: " + err.Error())
	}
}

func (cli *CLI) PrintPartitionTable() {
	nodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		fmt.Println("Error: " + err1.Error())
		return
	}

	pt, err := cli.cluster.CommManager.GetPartitionTableRequest(nodeId)

	if err != nil {
		fmt.Println("Error: " + err.Error())
		return
	}

	fmt.Println()

	t := table.NewWriter()

	t.AppendHeader(table.Row{"Node Id", "", "Start Token", "End Token", "REPLICATED TO"})

	var ordered = make(map[string][]partitioner.VNode)
	for _, v := range pt.VNodes {
		ordered[v.NodeId] = append(ordered[v.NodeId], v)
	}

	fl := true
	for nodeId, tokens := range ordered {

		if !fl {
			t.AppendRow(table.Row{"", "", "", ""})
		} else {
			fl = false
		}

		f := true
		counter := 1
		for i := 0; i < len(tokens); i++ {

			v := tokens[i]

			replicationList := ""
			for _, sn := range v.ReplicatedTo {
				if replicationList != "" {
					replicationList += ", " + sn
				} else {
					replicationList = sn
				}
			}

			if f {
				t.AppendRow(table.Row{nodeId, counter, v.StartToken, v.EndToken, replicationList})
				f = false
			} else {
				t.AppendRow(table.Row{"", counter, v.StartToken, v.EndToken, replicationList})
			}
			counter++
		}

	}

	t.SetCaption("Partition Table ID: " + strconv.FormatInt(pt.Timestamp, 10) + "\n")

	fmt.Println(t.Render())
}

func (cli *CLI) RequestUserConfirmation(message string, requireSpelling bool) bool {
	fmt.Print(message + " ")

	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	proceed := strings.Replace(text, "\n", "", -1)

	if requireSpelling {
		if proceed == "y" || proceed == "Y" || proceed == "yes" || proceed == "Yes" || proceed == "YES" {
			return true
		}
	} else if proceed == "" || proceed == "y" || proceed == "Y" {
		return true
	}

	return false
}

func (cli *CLI) SelectNode() (string, error) {
	remoteNodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		return "", err1
	}

	nodeId := ""
	pt, err := cli.cluster.CommManager.GetPartitionTableRequest(remoteNodeId)

	if err != nil {
		return "", err
	}

	var allNodes []string
	m := make(map[string]bool)
	for _, v := range pt.VNodes {
		if !m[v.NodeId] {
			allNodes = append(allNodes, v.NodeId)
			m[v.NodeId] = true
		}
	}
	allNodes = append(allNodes, "Cancel")

	prompt := promptui.Select{
		Label: "Select Node:",
		Items: allNodes,
	}

	_, result, err := prompt.Run()

	if err != nil {
		return "", err
	}

	if result == "Cancel" {
		return "", err
	} else {
		nodeId = result
	}

	return nodeId, nil
}

func (cli *CLI) RemoveNode(nodeId string) {
	var err error

	if nodeId == "" {
		nodeId, err = cli.SelectNode()
		if err != nil {
			fmt.Println("Error: " + err.Error())
			return
		}
	}

	remoteNodeId, _ := cli.GetClusterNodeId()

	goRemove := cli.RequestUserConfirmation("Are you sure you want to remove node "+nodeId+" from the cluster? (yes/No)", true)

	if goRemove {
		fmt.Println("Removing node " + nodeId + " from the cluster...")
		err := cli.cluster.CommManager.SendDecommissionNodeRequest(remoteNodeId, nodeId)
		if err != nil {
			fmt.Println("Error: " + err.Error())
			return
		}
		fmt.Println("Cluster started removing node " + nodeId + ", this operation may take a while to complete")
	}

}

func (cli *CLI) GetNodeStats(nodeId string) {

	var err error

	if nodeId == "" {
		nodeId, err = cli.SelectNode()
		if err != nil {
			fmt.Println("Error: " + err.Error())
			return
		}
	}

	var stats types.NodeStatsResponse
	stats, err = cli.cluster.CommManager.SendGetNodeStatsRequest(nodeId)

	if err != nil {
		fmt.Println(err)
		return
	}

	t := table.NewWriter()

	t.AppendHeader(table.Row{"Collection", "Number Of Items", "Number Of Upserts", "Number Of Reads"})

	for k, v := range stats.StatsPerCollection {
		t.AppendRow(table.Row{k, v.NumberOfEntries, v.NumberOfUpserts, v.NumberOfReads})
	}

	fmt.Println(t.Render())
}

func (cli *CLI) PrintClusterStatus() {
	remoteNodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		fmt.Println("Error: " + err1.Error())
		return
	}

	servers, err := cli.cluster.CommManager.SendClusterStatusRequest(remoteNodeId)

	if err != nil {
		fmt.Println("Error: " + err.Error())
		return
	}

	t := table.NewWriter()

	hasDecommisioned := false

	for _, s := range servers {
		if s.NodePort == "" && len(servers) > 1 {
			hasDecommisioned = true
			t.AppendRow(table.Row{s.NodeId, s.NodeIp, s.NodePort, s.NodeHeartbitPort, s.HeartbitStatus, s.PartitionTableTimestamp, s.Memory, s.CPU, "Decommissioned"})
		} else {
			t.AppendRow(table.Row{s.NodeId, s.NodeIp, s.NodePort, s.NodeHeartbitPort, s.HeartbitStatus, s.PartitionTableTimestamp, s.Memory, s.CPU + "%", ""})
		}
	}

	t.AppendHeader(table.Row{"Node Id", "IP", "Cluster Port", "Heartbit Port", "Status", "Partition Table", "Memory", "CPU", ""})

	if hasDecommisioned {
		t.SetCaption("Decommssioned nodes may still appear alive if the node was not shutdown\nMemory = Used/Free/Tot\n")
	} else {
		t.SetCaption("Memory = Used/Free/Tot\n")
	}

	fmt.Println(t.Render())
}

func (cli *CLI) GetNodeSyncStatus(nodeId string) {
	var err error

	if nodeId == "" {
		nodeId, err = cli.SelectNode()
		if err != nil {
			fmt.Println("Error: " + err.Error())
			return
		}
	}

	var stats types.DataSyncStatusResponse
	stats, err = cli.cluster.CommManager.SendGetSyncTasks(nodeId)

	if err != nil {
		fmt.Println(err)
		return
	}

	t := table.NewWriter()

	nothingToDo := true
	if len(stats.JobsQueue) > 0 {
		nothingToDo = false

		for _, job := range stats.JobsQueue {

			wfd := ""
			if job.WaitingToStartDelete {
				wfd = "Waiting to process deletes"
			}
			t.AppendRow(table.Row{job.PartitionTableTimestamp, wfd})

			t.AppendRow(table.Row{"TaskId", "Action", "Status", "FromNodeId", "ToNodeId", "Progress", "ProgressCollection", "StartToken", "EndToken", "Error"})
			for _, task := range job.SynchTasks {
				t.AppendRow(table.Row{task.ID, task.Action, task.Status, task.FromNodeId, task.ToNodeId, task.Progress, task.ProgressCollection, task.StartToken, task.EndToken, task.Error})
			}
		}
	}

	if len(stats.StreamingQueue) > 0 {
		nothingToDo = false
		t.AppendRow(table.Row{"Streaming Queue:"})
		t.AppendRow(table.Row{"TaskId", "Action", "Status", "FromNodeId", "ToNodeId", "Progress", "ProgressCollection", "StartToken", "EndToken", "Error"})
		for _, task := range stats.StreamingQueue {
			t.AppendRow(table.Row{task.ID, task.Action, task.Status, task.FromNodeId, task.ToNodeId, task.Progress, task.ProgressCollection, task.StartToken, task.EndToken, task.Error})
		}
	}

	if nothingToDo {
		fmt.Println("No sync tasks running")
	} else {
		fmt.Println(t.Render())
	}

}

func (cli *CLI) RetrySyncTask(taskId string) {

	nodeId, err := cli.SelectNode()
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return
	}

	if taskId == "" {
		fmt.Println("TaskId is required")
	} else {
		err = cli.cluster.CommManager.SendRetrySyncTask(nodeId, taskId)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("OK")
		}
	}

}

func (cli *CLI) PerformSet(collectionName string, key string, value string) {
	remoteNodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		fmt.Println("Error: " + err1.Error())
		return
	}
	err := cli.cluster.CommManager.SendSet(remoteNodeId, collectionName, []byte(key), []byte(value))
	if err != nil {
		fmt.Println("Error: " + err.Error())
	} else {
		fmt.Println("OK")
	}
}

func (cli *CLI) PerformGet(collectionName string, key string) {
	remoteNodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		fmt.Println("Error: " + err1.Error())
		return
	}

	resBytes, err := cli.cluster.CommManager.SendGet(remoteNodeId, collectionName, []byte(key))
	if err != nil {
		if err.Error() == "Not found" {
			fmt.Println("Not found")
		} else {
			fmt.Println("Error: " + err.Error())
		}

	} else {
		if len(resBytes) > 0 {
			fmt.Println(string(resBytes))
		} else {
			fmt.Println()
		}

	}
}

func (cli *CLI) PerformDel(collectionName string, key string) {
	remoteNodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		fmt.Println("Error: " + err1.Error())
		return
	}

	err := cli.cluster.CommManager.SendDel(remoteNodeId, collectionName, []byte(key))
	if err != nil {
		fmt.Println("Error: " + err.Error())
	} else {
		fmt.Println("OK")
	}
}

func (cli *CLI) PerformScan(collectionName string, startFromKey string, numberOfResults int, skipFirst bool, nodeId string) {
	var err error
	if nodeId == "" {
		nodeId, err = cli.SelectNode()
		if err != nil {
			fmt.Println("Error: " + err.Error())
			return
		}

	}

	data, err := cli.cluster.CommManager.SendScan(nodeId, collectionName, []byte(startFromKey), numberOfResults)
	if err != nil {
		fmt.Println("Error: " + err.Error())
	} else {
		if len(data) == 0 {
			fmt.Println(collectionName + " scan completed from " + nodeId)
		} else {
			t := table.NewWriter()
			t.AppendHeader(table.Row{"Key", "Value"})
			latestKey := ""
			hasData := false
			for _, item := range data {
				if skipFirst {
					skipFirst = false
				} else {
					hasData = true
					latestKey = string(item.Key)
					t.AppendRow(table.Row{latestKey, string(item.Value)})
				}
			}

			if hasData {
				fmt.Println()
				fmt.Println(t.Render())

				fmt.Print("Press enter to continue, or q to exit...")
				reader := bufio.NewReader(os.Stdin)
				text, _ := reader.ReadString('\n')
				ui := strings.Replace(text, "\n", "", -1)
				if ui == "q" {
					return
				} else {
					cli.PerformScan(collectionName, latestKey, numberOfResults, true, nodeId)
				}
			} else {
				fmt.Println(collectionName + " scan completed from " + nodeId)
			}

		}

	}
}

func (cli *CLI) PerformLocate(key string) {
	remoteNodeId, err1 := cli.GetClusterNodeId()

	if err1 != nil {
		fmt.Println("Error: " + err1.Error())
		return
	}

	loc, err := cli.cluster.CommManager.SendGetKeyLocation(remoteNodeId, []byte(key))
	if err != nil {
		fmt.Println("Error: " + err.Error())
	} else {
		fmt.Println("Master: " + loc.Master)
		for _, r := range loc.Replicas {
			fmt.Println("Replica: " + r)
		}
	}
}
