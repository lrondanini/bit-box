package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/lrondanini/bit-box/bitbox/actions"
	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/communication"
	"github.com/lrondanini/bit-box/bitbox/partitioner"
	"github.com/lrondanini/bit-box/bitbox/serverStatus"
	"github.com/lrondanini/bit-box/bitbox/serverStatus/heartBitStatus"
	"github.com/lrondanini/bit-box/bitbox/storage"

	"github.com/syndtr/goleveldb/leveldb"
)

type ClusterManager struct {
	currentNode              *Node
	commManager              *communication.CommunicationManager
	nodeCummunicationChannel chan actions.NodeActions
	eventsManager            *EventsManager
	partitionTable           partitioner.PartitionTable
	systemDb                 *storage.Collection
	servers                  map[string]server.Server
	currentServerStatus      serverStatus.ServerStatus

	logger *utils.Logger

	topologyManager *TopologyManager
}

func InitClusterManager(currentNode *Node) (*ClusterManager, error) {
	sysCollection, err := storage.OpenCollection("system")

	if err != nil {
		return nil, err
	}

	var clusterManager ClusterManager = ClusterManager{
		currentNode:              currentNode,
		nodeCummunicationChannel: make(chan actions.NodeActions),
		systemDb:                 sysCollection,
		logger:                   utils.GetLogger(),
		currentServerStatus:      serverStatus.Starting,
	}

	clusterManager.eventsManager = initEventsManager(currentNode.GetId(), &clusterManager)

	pt, errDB := partitioner.LoadFromDb(sysCollection)
	if errDB != nil {
		if errDB == leveldb.ErrNotFound {
			pt = partitioner.InitEmptyPartitionTable()
		} else {
			return nil, errDB
		}
	}
	clusterManager.partitionTable = *pt

	clusterManager.servers = server.InitServerList(&clusterManager.partitionTable)

	partitioner.PrintVnodes(clusterManager.partitionTable.VNodes)
	fmt.Println("Timestamp:", clusterManager.partitionTable.Timestamp)
	fmt.Println("ServerList:", clusterManager.servers)

	clusterManager.topologyManager = InitTopologyManager(&clusterManager)
	return &clusterManager, nil
}

func (cm *ClusterManager) StartCommunications() chan actions.NodeActions {
	cm.commManager = communication.StartCommManager(cm.currentNode.GetId(), cm.currentNode.NodeIp, cm.currentNode.NodePort)

	go func() {
		for {
			msg, ok := <-cm.commManager.ReceiverChannel /// msg is of type tcp.MessageFromCluster
			if ok {
				cm.eventsManager.HandleEvent(&msg)
			} else {
				//channel closed
				return
			}

		}
	}()

	return cm.nodeCummunicationChannel
}

func (cm *ClusterManager) Shutdown() {
	cm.commManager.Shutdown()
}

func (cm *ClusterManager) JoinCluster(forceRejoin bool) error {

	currentServer := cm.servers[cm.currentNode.GetId()]

	if currentServer.NodeId == cm.currentNode.GetId() {
		//start heartbit manager
		cm.currentNode.StartHeartbit()
		cm.currentServerStatus = serverStatus.Alive

		servers := cm.currentNode.GetHeartbitStatus()
		nodeWithMostRecentPtTimestamp := ""
		var remotePtTimestamp int64
		remotePtTimestamp = 0
		for _, s := range servers {
			if s.NodeId != cm.currentNode.GetId() {
				ptTimestamp, err := strconv.ParseInt(s.PartitionTable, 10, 64)
				if err != nil {
					fmt.Println("Cannot parse PT timestamp from " + s.NodeId + " (" + s.PartitionTable + "):" + err.Error())
				} else {
					if ptTimestamp > remotePtTimestamp {
						nodeWithMostRecentPtTimestamp = s.NodeId
						remotePtTimestamp = ptTimestamp
					}
				}
			}
		}

		if remotePtTimestamp > cm.partitionTable.Timestamp {
			//request PT from nodeWithMostRecentPtTimestamp
			partitionTable, err := cm.commManager.GetPartitionTableRequest(nodeWithMostRecentPtTimestamp)
			if err != nil {
				panic("Detected a newer partition table but cannot retrieve it from the node that has it: " + nodeWithMostRecentPtTimestamp + " - " + err.Error())
			} else {
				csServers := server.InitServerList(partitionTable)
				s := csServers[cm.currentNode.GetId()]
				if s.NodeId == "" && !forceRejoin {
					fmt.Println()
					fmt.Println("ERROR: Cannot find current node in the current partition table - was this node decommissioned?")
					fmt.Println("If you want to reinstate this node, use the -rejoin flag to force rejoin the cluster")
					fmt.Println()
					return errors.New("node not in the partition table")
				} else if s.NodeId == "" {
					//forceJoin
					err := cm.JoinClusterAsNewNode()
					if err != nil {
						return err
					}
				} else {
					err := cm.updatePartitionTable(partitionTable)
					if err != nil {
						panic("Cannot save new partition table: " + err.Error())
					} else {
						//notify current node
						cm.nodeCummunicationChannel <- actions.NewPartitionTable
					}
				}
			}
		}
	} else {
		//new node is joining the cluster
		err := cm.BootstrapNewNode()
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *ClusterManager) BootstrapNewNode() error {
	var conf = utils.GetClusterConfiguration()

	var isFirstNode bool = false
	if conf.CLUSTER_NODE_IP == "" {
		fmt.Println("Creating a new cluster")
		isFirstNode = true
	} else {
		if conf.CLUSTER_NODE_IP == "" || conf.CLUSTER_NODE_PORT == "" {
			panic("No clusterNodeIp and clusterNodePort in configuration file")
		}
		_, err := cm.commManager.SendPing(GenerateNodeId(conf.CLUSTER_NODE_IP, conf.CLUSTER_NODE_PORT))
		if err != nil {
			if len(cm.servers) == 0 {
				//first node of a new cluster but conf has already set CLUSTER_NODE_IP
				isFirstNode = true
			} else {
				return errors.New("[cluster-1] Cannot ping " + conf.CLUSTER_NODE_IP + ":" + conf.CLUSTER_NODE_PORT + ":" + err.Error())
			}
		}
	}

	if !isFirstNode {
		err := cm.JoinClusterAsNewNode()
		if err != nil {
			return err
		}
	} else {
		fmt.Print("Initialing new cluster...")

		//creating a brand new partition table
		vnodes := partitioner.GeneratePartitionTable(1, cm.currentNode.GetId(), cm.currentNode.NodeIp, cm.currentNode.NodePort)
		err := cm.updatePartitionTable(partitioner.InitPartitionTable(*vnodes, time.Now().UnixMicro()))
		if err != nil {
			return err
		}
		fmt.Println("DONE")
		cm.currentNode.StartHeartbit()
		cm.currentServerStatus = serverStatus.Alive
	}

	return nil
}

func (cm *ClusterManager) JoinClusterAsNewNode() error {
	var conf = utils.GetClusterConfiguration()

	//contact the other cluster node asking to join
	fmt.Print("Send join cluster request...")

	err := cm.commManager.SendJoinClusterRequest(GenerateNodeId(conf.CLUSTER_NODE_IP, conf.CLUSTER_NODE_PORT), cm.currentNode.GetId(), cm.currentNode.NodeIp, cm.currentNode.NodePort)
	if err != nil {
		return err
	}

	fmt.Println("DONE")
	fmt.Println("Waiting for cluster (this operation may take a while)...")
	//the node in charge of reconfiguring the cluster will contact this node providing the  partition table
	cm.currentServerStatus = serverStatus.Joining

	return nil
}

func (cm *ClusterManager) updatePartitionTable(pt *partitioner.PartitionTable) error {
	cm.partitionTable = *pt
	cm.servers = server.InitServerList(&cm.partitionTable)

	err := cm.partitionTable.SaveToDb(cm.systemDb)
	if err != nil {
		return err
	}

	//start heatbit manager
	cm.currentServerStatus = serverStatus.Alive
	return nil
}

func (cm *ClusterManager) StartAddNewNode(reqFromNodeId string, newNodeId string, newNodeIp string, newNodePort string) error {

	//verify that newNodeId is not already in our cluster
	if _, ok := cm.servers[newNodeId]; ok {
		return errors.New("node " + newNodeId + " already in the cluster")
	}

	if cm.currentServerStatus != serverStatus.Alive {
		return errors.New(cm.currentNode.GetId() + " cannot process your request, node is " + cm.currentServerStatus.String())
	}

	cm.topologyManager.StartAddNewNode(reqFromNodeId, newNodeId, newNodeIp, newNodePort)
	return nil
}

func (cm *ClusterManager) StartDecommissionNode(reqFromNodeId, nodeId string) error {

	//verify that newNodeId is in our cluster
	if _, ok := cm.servers[nodeId]; !ok {
		return errors.New("node " + nodeId + " not in the cluster")
	}

	return cm.topologyManager.StartDecommissionNode(reqFromNodeId, nodeId)
}

func (cm *ClusterManager) SetNewClusterMaster(fromNodeId string) (bool, int64) {
	return cm.topologyManager.SetNewClusterMaster(fromNodeId)
}

func (cm *ClusterManager) manageUpdatePartitionTableRequest(newPartitionTable *partitioner.PartitionTable) {
	cm.topologyManager.manageUpdatePartitionTableRequest(newPartitionTable)
}

func (cm *ClusterManager) manageAbortPartitionTableChangesRequest(message string) {
	if cm.currentServerStatus == serverStatus.Joining {
		cm.logger.Error().Msg(message)
		cm.nodeCummunicationChannel <- actions.Shutdown
	}
}

func (cm *ClusterManager) manageReleaseMasterRequest(fromNodeId string) {
	cm.topologyManager.releaseMaster(fromNodeId, false)
}

func (cm *ClusterManager) manageForceReleaseMasterRequest(fromNodeId string) {
	cm.topologyManager.releaseMaster(fromNodeId, true)
}

func (cm *ClusterManager) manageCommitPartitionTableRequest(fromNodeId string) {
	cm.topologyManager.manageCommitPartitionTableRequest(fromNodeId)
}

func (cm *ClusterManager) GetClusterStatus() []server.Server {
	var res []server.Server
	servers := cm.currentNode.GetHeartbitStatus()
	for _, s := range servers {
		cs := cm.servers[s.NodeId]
		s.NodePort = cs.NodePort
		res = append(res, s)
	}
	return res

}

func (cm *ClusterManager) VerifyNodeCrash() {
	//if temp Master crashed, we need to free the master role
	servers := cm.currentNode.GetHeartbitStatus()
	for _, s := range servers {
		if s.HeartbitStatus == heartBitStatus.Failed || s.HeartbitStatus == heartBitStatus.Left || s.HeartbitStatus == heartBitStatus.Leaving || s.HeartbitStatus == heartBitStatus.None {
			if s.NodeId != cm.currentNode.GetId() {
				if cm.currentServerStatus == serverStatus.Joining {
					//this never happens because a joining node did not start the heartbit yet
					panic("Could not join cluster, node " + s.NodeId + " crashed")
				} else {
					cm.topologyManager.HandlePossibleMasterCrash(s.NodeId)
				}
			}
		}
	}
}
