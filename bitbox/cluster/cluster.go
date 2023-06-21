package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lrondanini/bit-box/bitbox/cluster/actions"
	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/server/serverStatus"
	"github.com/lrondanini/bit-box/bitbox/cluster/server/serverStatus/heartBitStatus"
	"github.com/lrondanini/bit-box/bitbox/cluster/stream"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/communication"
	"github.com/lrondanini/bit-box/bitbox/communication/types"
	"github.com/lrondanini/bit-box/bitbox/storage"
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
	dataSyncManager          *DataSyncManager
	logger                   *utils.InternalLogger
	topologyManager          *TopologyManager
	sync                     sync.Mutex
}

func InitClusterManager(currentNode *Node) (*ClusterManager, error) {
	sysCollection, err := storage.OpenCollection(storage.SYSTEM_DB_NAME)

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
		if errDB == storage.ErrKeyNotFound {
			pt = partitioner.InitEmptyPartitionTable()
		} else {
			return nil, errDB
		}
	}
	clusterManager.partitionTable = *pt

	//partitioner.PrintVnodes(pt.VNodes)

	clusterManager.servers = server.InitServerList(&clusterManager.partitionTable)

	fmt.Println("Current Partition Table:", clusterManager.partitionTable.Timestamp)

	clusterManager.topologyManager = InitTopologyManager(&clusterManager)

	clusterManager.dataSyncManager, errDB = InitDataSyncManager(&clusterManager)
	if errDB != nil {
		return nil, errDB
	}
	return &clusterManager, nil
}

func (cm *ClusterManager) CanSetHeartbitPartionTable() bool {
	return cm.dataSyncManager.CanSetPartitionTable()
}

func (cm *ClusterManager) GetCurrentNode() *Node {
	return cm.currentNode
}

func (cm *ClusterManager) GetSystemDB() *storage.Collection {
	return cm.systemDb
}

func (cm *ClusterManager) StartCommunications() chan actions.NodeActions {
	cm.commManager = communication.StartCommManager(cm.currentNode.GetId(), cm.currentNode.NodeIp, cm.currentNode.NodePort)

	go func() {
		for {
			msg, ok := <-cm.commManager.ReceiverChannel /// msg is of type tcp.MessageFromCluster
			if ok {
				go cm.eventsManager.HandleEvent(msg)
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
	cm.systemDb.Close()
}

func (cm *ClusterManager) GetServers() map[string]server.Server {
	cm.sync.Lock()
	defer cm.sync.Unlock()
	return cm.servers

}

func (cm *ClusterManager) UpdateServers(servers map[string]server.Server) {
	cm.sync.Lock()
	defer cm.sync.Unlock()
	cm.servers = servers
}

func (cm *ClusterManager) initServerList() {
	cm.sync.Lock()
	defer cm.sync.Unlock()
	cm.servers = server.InitServerList(&cm.partitionTable)
}

func (cm *ClusterManager) UpdateServersHeartbitStatus() {
	servers := cm.currentNode.GetHeartbitStatus()
	cm.initServerList()
	clusterServers := cm.GetServers()
	for sid, s := range clusterServers {
		found := false
		for _, hbs := range servers {
			if hbs.NodeId == sid {
				s.HeartbitStatus = hbs.HeartbitStatus
				s.PartitionTableTimestamp = hbs.PartitionTableTimestamp
				s.Memory = hbs.Memory
				s.CPU = hbs.CPU
				s.NumbOfVNodes = hbs.NumbOfVNodes

				found = true
			}
		}

		if !found {
			s.HeartbitStatus = heartBitStatus.Failed
		}

		clusterServers[sid] = s
	}
	cm.UpdateServers(clusterServers)

	cm.dataSyncManager.VerifyClusterSyncWihtPartionTable()

}

func (cm *ClusterManager) JoinCluster(forceRejoin bool) error {

	servers := cm.GetServers()
	currentServer := servers[cm.currentNode.GetId()]

	if currentServer.NodeId == cm.currentNode.GetId() {
		//start heartbit manager
		cm.currentNode.StartHeartbit()
		if cm.CanSetHeartbitPartionTable() {
			cm.currentNode.UpdateHeartbitPartitionTable(cm.partitionTable.Timestamp)
		}
		cm.currentServerStatus = serverStatus.Alive

		servers := cm.currentNode.GetHeartbitStatus()
		nodeWithMostRecentPtTimestamp := ""
		var remotePtTimestamp int64
		remotePtTimestamp = 0
		for _, s := range servers {
			if s.NodeId != cm.currentNode.GetId() {
				ptTimestamp := s.PartitionTableTimestamp
				if ptTimestamp > remotePtTimestamp {
					nodeWithMostRecentPtTimestamp = s.NodeId
					remotePtTimestamp = ptTimestamp
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
					//forceJoin is set to true
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
			servers := cm.GetServers()
			if len(servers) == 0 {
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
		vnodes := partitioner.GenerateNewPartitionTable(conf.NUMB_VNODES, cm.currentNode.GetId(), cm.currentNode.NodeIp, cm.currentNode.NodePort)
		//first time adding a pt
		err := cm.initPartitionTable(partitioner.InitPartitionTable(*vnodes, time.Now().UnixMicro()))
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

	//reset node to a clean state
	cm.partitionTable = *partitioner.InitEmptyPartitionTable()

	var conf = utils.GetClusterConfiguration()

	//contact the other cluster node asking to join
	fmt.Print("Send join cluster request...")

	err := cm.commManager.SendJoinClusterRequest(GenerateNodeId(conf.CLUSTER_NODE_IP, conf.CLUSTER_NODE_PORT), cm.currentNode.GetId(), cm.currentNode.NodeIp, cm.currentNode.NodePort, conf.NUMB_VNODES)
	if err != nil {
		return err
	}

	fmt.Println("DONE")
	fmt.Println("Waiting for cluster (this operation may take a while)...")
	//the node in charge of reconfiguring the cluster will contact this node providing the  partition table
	cm.currentServerStatus = serverStatus.Joining

	return nil
}

func (cm *ClusterManager) initPartitionTable(pt *partitioner.PartitionTable) error {

	cm.dataSyncManager.InitUpdatePartitionTableProcess(pt)
	cm.sync.Lock()
	cm.partitionTable = *pt
	cm.sync.Unlock()
	err := cm.partitionTable.SaveToDb(cm.systemDb)
	if err != nil {
		//TODO: what to do here?
		return err
	}

	cm.initServerList()
	cm.currentServerStatus = serverStatus.Alive
	cm.dataSyncManager.ProcessNextGetDataTask()

	return nil
}

func (cm *ClusterManager) updatePartitionTable(pt *partitioner.PartitionTable) error {
	err := cm.initPartitionTable(pt)

	if err != nil {
		return err
	}

	//notify current node
	cm.nodeCummunicationChannel <- actions.NewPartitionTable

	return nil
}

func (cm *ClusterManager) StartAddNewNode(reqFromNodeId string, newNodeId string, newNodeIp string, newNodePort string, numberOfVNodes int) error {

	//verify that newNodeId is not already in our cluster
	servers := cm.GetServers()
	if _, ok := servers[newNodeId]; ok {
		return errors.New("node " + newNodeId + " already in the cluster")
	}

	if cm.currentServerStatus != serverStatus.Alive {
		return errors.New(cm.currentNode.GetId() + " cannot process your request, node is " + cm.currentServerStatus.String())
	}

	cm.topologyManager.StartAddNewNode(reqFromNodeId, newNodeId, newNodeIp, newNodePort, numberOfVNodes)
	return nil
}

func (cm *ClusterManager) StartDecommissionNode(reqFromNodeId, nodeId string) error {

	//verify that newNodeId is in our cluster
	servers := cm.GetServers()
	if _, ok := servers[nodeId]; !ok {
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
		cm.logger.Error(nil, message)
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
	clusterServers := cm.GetServers()
	for _, s := range servers {
		cs := clusterServers[s.NodeId]
		s.NodePort = cs.NodePort
		res = append(res, s)
	}
	return res

}

func (cm *ClusterManager) manageStartDataStreamRequest(toNodeId string, taskId string, from uint64, to uint64) {
	cm.dataSyncManager.AddStreamingTask(taskId, toNodeId, from, to)
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

func (cm *ClusterManager) NodeReadyForWork() {
	cm.dataSyncManager.StartStreaming()
	cm.dataSyncManager.ProcessNextGetDataTask()
	cm.dataSyncManager.VerifyClusterSyncWihtPartionTable()
}

func (cm *ClusterManager) manageDataStreamChunk(taskId string, collectionName string, progress uint64, data []stream.StreamEntry) {
	cm.dataSyncManager.SaveNewDataChunk(taskId, collectionName, progress, data)
}

func (cm *ClusterManager) manageDataStreamTaskCompleted(fromNodeId string, taskId string) {
	cm.dataSyncManager.TaskCompleted(taskId)
}

func (cm *ClusterManager) manageSendGetSyncTasks() types.DataSyncStatusResponse {
	return cm.dataSyncManager.GetStatus()
}

func (cm *ClusterManager) manageRetrySyncTask(taskId string) {
	cm.dataSyncManager.ForceRetryStreamingTask(taskId)
}

func (cm *ClusterManager) manageSet(fromNodeId string, collectionname string, key []byte, value []byte) error {
	return cm.currentNode.UpsertRaw(fromNodeId, collectionname, key, value)
}

func (cm *ClusterManager) manageGet(fromNodeId string, collectionname string, key []byte) ([]byte, error) {
	return cm.currentNode.GetRaw(fromNodeId, collectionname, key)
}

func (cm *ClusterManager) manageDel(fromNodeId string, collectionname string, key []byte) error {
	return cm.currentNode.DeleteRaw(fromNodeId, collectionname, key)
}

func (cm *ClusterManager) manageScan(collectionname string, startFromKey []byte, numberOfResults int) ([]types.RWRequest, error) {
	return cm.currentNode.ScanRaw(collectionname, startFromKey, numberOfResults)
}

func (cm *ClusterManager) manageGetKeyLocation(key []byte) partitioner.HashLocation {
	return cm.currentNode.GetKeyLocationInCluster(key)
}

func (cm *ClusterManager) manageActionsLogStreamChunk(log []storage.Entry) {
	cm.currentNode.processActionsLog(log)
}
