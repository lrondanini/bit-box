package cluster

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/lrondanini/bit-box/bitbox/actions"
	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"

	"github.com/hashicorp/serf/serf"
)

type Node struct {
	id              string
	NodeIp          string
	NodePort        string
	clusterManager  *ClusterManager
	heartbitManager *HeartbitManager
	logger          *utils.Logger
}

func GenerateNodeId(nodeIp string, nodePort string) string {
	return nodeIp + ":" + nodePort
}

func InitNode(conf utils.Configuration) (*Node, error) {
	utils.VerifyAndSetConfiguration(&conf)

	var node Node = Node{
		id:       GenerateNodeId(conf.NODE_IP, conf.NODE_PORT),
		NodeIp:   conf.NODE_IP,
		NodePort: conf.NODE_PORT,
		logger:   utils.GetLogger(),
	}

	cm, err := InitClusterManager(&node)
	if err != nil {
		return nil, err
	}

	node.clusterManager = cm

	node.heartbitManager = InitHeartbitManager(node.id, conf.NODE_HEARTBIT_PORT)

	//node.loadDataFromDisk()

	return &node, nil
}

func (n *Node) GetId() string {
	return n.id
}

func (n *Node) Start(ctx context.Context, signalChan chan os.Signal, forceRejoin bool) error {
	ch := n.clusterManager.StartCommunications()
	defer n.Shutdown()

	err := n.clusterManager.JoinCluster(forceRejoin)

	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if ok {
				switch msg {
				case actions.Shutdown:
					signalChan <- os.Interrupt
				case actions.NewPartitionTable:
					//if node started in standalone mode, it needs to start heartbit manager
					if !n.heartbitManager.started {
						n.StartHeartbit()
					}
					n.onNewPartitionTable()
				}
			}
		default:
			//do your work
			//fmt.Println("Doing work")
			//time.Sleep(2 * time.Second)
			//work() //on defer call shutdown node
		}
	}
}

func (n *Node) Shutdown() {
	fmt.Println("Shutting node down...")
	n.heartbitManager.Shutdown()
	n.clusterManager.Shutdown()
	fmt.Println("...cya!")
}

func (n *Node) StartHeartbit() {
	fmt.Print("Starting heartbit...")
	if !n.heartbitManager.started && len(n.clusterManager.servers) > 1 {
		n.heartbitManager.JoinCluster(n.clusterManager.partitionTable.Timestamp)
		if n.heartbitManager.started {
			go n.manageHeartbitEvents()
		}
	} else {
		n.heartbitManager.SetPartitionTableTimestamp(n.clusterManager.partitionTable.Timestamp)
		n.heartbitManager.UpdateHardwareStats()

	}
	fmt.Println("DONE")
}

func (n *Node) GetHeartbitStatus() []server.Server {
	return n.heartbitManager.GetServers()
}

func (n *Node) onNewPartitionTable() {
	n.StartHeartbit()
	n.logger.Info().Msg("New partition table received: " + strconv.FormatInt(n.clusterManager.partitionTable.Timestamp, 10))
	n.heartbitManager.SetPartitionTableTimestamp(n.clusterManager.partitionTable.Timestamp)
}

func (n *Node) manageHeartbitEvents() {
	for {
		r := <-n.heartbitManager.eventChannel

		switch r.EventType() {
		case serf.EventMemberLeave:
			n.clusterManager.VerifyNodeCrash()
		}

		/*

			USEFUL NOTES: NOTE AS A QUERY CAN "ANSWER" WHILE USER EVENTS CAN ONLY RECEIVE

			******** to broadcast a query:
			############## SENDER ############
			r, e := serf.Query("test-query", []byte("test-payload"), serf.DefaultQueryParams())
			if e != nil {
				fmt.Println(e)
			}
			qr := <-r.ResponseCh()
			fmt.Println(qr.From, string(qr.Payload))

			############## RECEIVER - here catch it as ############
			fmt.Println(r)
			q, ok := r.(*serf.Query)
			if !ok {
				continue
			}

			fmt.Println(q.Name, string(q.Payload))
			e := q.Respond([]byte("test-response"))
			if e != nil {
				fmt.Println(e)
			}
			**********************


			******** to broadcast a user events:
			############## SENDER ############
			serf.UserEvent("test-event", []byte("test-payload"), false)

			############## RECEIVER - here catch it as ########################
			u, ok := r.(serf.UserEvent)
			if !ok {
				continue
			}

			fmt.Println(u.Name, string(u.Payload))
			**********************
		*/
	}
}

// func (n *Node) loadDataFromDisk() {
// 	conf := utils.GetConfiguration()

// 	//Verify that data folder exists
// 	dataFolderPath := conf.DATA_FOLDER
// 	if dataFolderPath == "" {
// 		dataFolderPath = "./data"
// 	}
// 	if _, err := os.Stat(dataFolderPath); errors.Is(err, os.ErrNotExist) {
// 		fmt.Println("Creating data folder: " + dataFolderPath)
// 		err := os.Mkdir(dataFolderPath, os.ModePerm)
// 		if err != nil {
// 			panic("could not create folder " + dataFolderPath)
// 		}
// 	}

// 	//load node details from file
// }
