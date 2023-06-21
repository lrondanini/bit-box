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

package cluster

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/server/serverStatus"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
)

type TopologyManager struct {
	logger *utils.InternalLogger

	clusterManager *ClusterManager

	abortAllOperations chan bool //used in case of a node gets stacked in a configuration change - CURENTLY NOT USED

	//used to queue requests to add/remove nodes
	isChangingConfiguration  bool
	configurationJobChannel  chan server.Server
	configurationLock        sync.Mutex
	tempPartitionTable       partitioner.PartitionTable //temporary partition table used during configuration changes
	currentMasterNodeId      string
	waitingForCommit         bool
	masterReleaseReqReceived chan bool
	waitForMasterRelease     bool

	requestsRouter map[string]string //used to send requests/responses to the right node (eg cli requesting node decomissioning)
}

func InitTopologyManager(clusterManager *ClusterManager) *TopologyManager {

	var topologyManager TopologyManager = TopologyManager{
		clusterManager:           clusterManager,
		logger:                   utils.GetLogger(),
		masterReleaseReqReceived: make(chan bool),
		abortAllOperations:       make(chan bool),
		requestsRouter:           make(map[string]string),
	}

	return &topologyManager
}

func (tm *TopologyManager) StartAddNewNode(reqFromNodeId string, newNodeId string, newNodeIp string, newNodePort string, numberOfVNodes int) {
	tm.configurationLock.Lock()
	if reqFromNodeId == "" {
		tm.requestsRouter[newNodeId] = newNodeId
	} else {
		tm.requestsRouter[newNodeId] = reqFromNodeId
	}
	tm.configurationLock.Unlock()

	s := server.Server{
		NodeId:       newNodeId,
		NodeIp:       newNodeIp,
		NodePort:     newNodePort,
		Status:       serverStatus.Joining,
		NumbOfVNodes: numberOfVNodes,
	}

	tm.logger.Info("New node requested to join cluster:" + newNodeId + " (" + newNodeIp + ":" + newNodePort + ")")
	tm.startChangeClusterTopologyQueue(s)
}

func (tm *TopologyManager) StartDecommissionNode(reqFromNodeId string, nodeId string) error {
	tm.configurationLock.Lock()
	if reqFromNodeId == "" {
		tm.requestsRouter[nodeId] = nodeId
	} else {
		tm.requestsRouter[nodeId] = reqFromNodeId
	}
	tm.configurationLock.Unlock()

	s := server.Server{
		NodeId: nodeId,
		Status: serverStatus.Decommissioning,
	}
	found := false
	servers := tm.clusterManager.GetServers()
	for _, server := range servers {
		if server.NodeId == nodeId {
			s.NodeIp = server.NodeIp
			s.NodePort = server.NodePort
			found = true
		}
	}

	if !found {
		return errors.New("Node not found")
	}

	tm.logger.Info("Node requested to leave the cluster:" + nodeId)
	tm.startChangeClusterTopologyQueue(s)

	return nil
}

func (tm *TopologyManager) NotifyNodeStartup(nodeId string, nodeIp string, nodePort string) (string, int64, error) {
	nodeWithMostRecentPtTimestamp := ""
	var remotePtTimestamp int64
	remotePtTimestamp = 0
	servers := tm.clusterManager.GetServers()
	for _, server := range servers {
		if server.NodeId != tm.clusterManager.currentNode.GetId() {
			ptTimestamp, err := tm.clusterManager.commManager.SendNodeBackOnlineNotification(server.NodeId)

			if err != nil {
				return "", 0, err
			}

			if ptTimestamp > remotePtTimestamp {
				nodeWithMostRecentPtTimestamp = server.NodeId
				remotePtTimestamp = ptTimestamp
			}

		}
	}
	return nodeWithMostRecentPtTimestamp, remotePtTimestamp, nil
}

func (tm *TopologyManager) startChangeClusterTopologyQueue(s server.Server) {
	tm.configurationLock.Lock()
	if tm.isChangingConfiguration {
		tm.configurationLock.Unlock()
		//this will block when capacity is reached (10 servers have to request to join/leave on this channel)
		tm.configurationJobChannel <- s
	} else {
		tm.isChangingConfiguration = true
		tm.configurationLock.Unlock()
		tm.configurationJobChannel = make(chan server.Server, 10) //add capacity so that the channel is not blocked and server can respond
		go tm.changeClusterTopologyQueue(tm.configurationJobChannel)
		tm.configurationJobChannel <- s
	}
}

func (tm *TopologyManager) changeClusterTopologyQueue(jobQueue chan server.Server) {
	for {
		select {
		case server, ok := <-jobQueue:
			if ok {
				tm.changeClusterTopology(server)
				infoMsg := ""
				if server.Status == serverStatus.Joining {
					infoMsg = "Toplogy successfully changed, added node: " + server.NodeId
				} else if server.Status == serverStatus.Decommissioning {
					infoMsg = "Toplogy successfully changed, removed node: " + server.NodeId
				}
				tm.logger.Info(infoMsg)

			} else {
				//all done
				return
			}
		default:
			tm.configurationLock.Lock()
			tm.isChangingConfiguration = false
			tm.configurationLock.Unlock()
			close(jobQueue)
		}
	}
}

func (tm *TopologyManager) changeClusterTopology(s server.Server) {
	if tm.waitingForCommit {
		tm.logger.Info("Waiting for commit and master relase...")

		tm.configurationLock.Lock()
		tm.waitForMasterRelease = true
		tm.configurationLock.Unlock()
		select {
		case <-tm.masterReleaseReqReceived:
			//continue
			tm.logger.Info("Master released, resuming operations")
		case <-tm.abortAllOperations:
			return
		}
	}

	//for convinience we use tempPartitionTable for our calculations
	tm.tempPartitionTable = tm.clusterManager.partitionTable

	tm.logger.Info("Starting MASTER procedure")

	abort := tm.startBecomeMasterProcedure(s.NodeId)
	if abort {
		return
	}
	tm.logger.Info("MASTER procedure successful")
	//time.Sleep(10 * time.Second) // used for testing

	tm.calculateAndSubmitPartitionTable(s, tm.tempPartitionTable.VNodes)
}

func (tm *TopologyManager) startBecomeMasterProcedure(nodeRequestingId string) bool {
	tm.waitForMasterRelease = false
	allTimestamps := make(map[string]int64)
	servers := tm.clusterManager.GetServers()
	for _, server := range servers {
		if server.NodeId != tm.clusterManager.currentNode.GetId() && server.NodeId != nodeRequestingId {
			accepted, ptTimestamp, err := tm.clusterManager.commManager.SendRequestToBecomeMaster(server.NodeId)
			if err != nil {
				tm.logger.Error(err, "Could not send master request to "+server.NodeId+"("+server.NodeIp+":"+server.NodePort+")")

				//abort the process and send a message to the requestor saying that the process could not be completed
				abortMessage := "Error trying to reach " + server.NodeId + "(" + server.NodeIp + ":" + server.NodePort + "): " + err.Error()
				tm.configurationLock.Lock()
				sendTo := tm.requestsRouter[nodeRequestingId]
				tm.configurationLock.Unlock()
				tm.clusterManager.commManager.SendAbortPartitionTableChangesToRequestor(sendTo, abortMessage)
				return true
			}

			if accepted {
				allTimestamps[server.NodeId] = ptTimestamp
			} else {
				tm.waitForMasterRelease = true
				break
			}
		}
	}

	if tm.waitForMasterRelease {
		tm.logger.Info("Another MASTER detected, waiting for release")
		select {
		case <-tm.masterReleaseReqReceived:
			//retry:
			tm.logger.Info("Master released, resuming operations")
			return tm.startBecomeMasterProcedure(nodeRequestingId)
		case <-tm.abortAllOperations:
			return false
		}
	}

	//verify current partition table timestamp (safety step)

	newestPtTimestamp := tm.clusterManager.partitionTable.Timestamp
	newestPtTimestampBelongsTo := tm.clusterManager.currentNode.GetId()

	for nodeId, ptTimestamp := range allTimestamps {
		if newestPtTimestamp < ptTimestamp {
			newestPtTimestamp = ptTimestamp
			newestPtTimestampBelongsTo = nodeId
		}
	}

	if newestPtTimestampBelongsTo != tm.clusterManager.currentNode.GetId() {
		//fetch the newest partition table
		partitionTable, err := tm.clusterManager.commManager.GetPartitionTableRequest(newestPtTimestampBelongsTo)
		if err != nil {

			abortMessage := "Detected newer partition table but could not fetch it from " + newestPtTimestampBelongsTo + ": " + err.Error()
			tm.configurationLock.Lock()
			sendTo := tm.requestsRouter[nodeRequestingId]
			tm.configurationLock.Unlock()
			tm.clusterManager.commManager.SendAbortPartitionTableChangesToRequestor(sendTo, abortMessage)
			return true
		}

		tm.tempPartitionTable = *partitionTable
	}

	return false
}

func (tm *TopologyManager) SetNewClusterMaster(fromNodeId string) (bool, int64) {
	currentPartitionTableTimestamp := tm.clusterManager.partitionTable.Timestamp
	accepted := true
	tm.configurationLock.Lock()
	if tm.waitingForCommit {
		tm.logger.Info("DECLINED MASTER REQUEST - from " + fromNodeId + " - waiting for commit: " + strconv.FormatBool(tm.waitingForCommit) + " - current master: " + tm.currentMasterNodeId)
		accepted = false
	} else {
		tm.logger.Info("ACCEPTED MASTER REQUEST - from " + fromNodeId)
		tm.waitingForCommit = true
		tm.currentMasterNodeId = fromNodeId
	}
	tm.configurationLock.Unlock()

	return accepted, currentPartitionTableTimestamp
}

func (tm *TopologyManager) calculateAndSubmitPartitionTable(s server.Server, currentVNodes []partitioner.VNode) {
	abort := false

	if s.Status == serverStatus.Joining {
		newVNodes, err := partitioner.AddNode(&currentVNodes, s.NumbOfVNodes, s.NodeId, s.NodeIp, s.NodePort)
		if err != nil {
			//abort the process and send a message to the requestor saying that the process could not be completed
			abortMessage := "Could not create partition table for " + s.NodeId + "(" + s.NodeIp + ":" + s.NodePort + "): " + err.Error()
			tm.logger.Error(err, "Could not create partition table for "+s.NodeId+"("+s.NodeIp+":"+s.NodePort+")")
			tm.configurationLock.Lock()
			sendTo := tm.requestsRouter[s.NodeId]
			tm.configurationLock.Unlock()
			tm.clusterManager.commManager.SendAbortPartitionTableChangesToRequestor(sendTo, abortMessage)
			abort = true
		} else {
			tm.configurationLock.Lock()
			tm.tempPartitionTable = *partitioner.InitPartitionTable(*newVNodes, time.Now().UnixMicro()+int64(rand.Intn(1000)))
			tm.configurationLock.Unlock()
			tm.logger.Info("Start adding:" + s.NodeId + " (" + s.NodeIp + ":" + s.NodePort + ") - New partition table:" + strconv.FormatInt(tm.tempPartitionTable.Timestamp, 10))
		}

	} else if s.Status == serverStatus.Decommissioning {
		newVNodes := partitioner.RemoveNode(&currentVNodes, s.NodeId)
		tm.configurationLock.Lock()
		tm.tempPartitionTable = *partitioner.InitPartitionTable(*newVNodes, time.Now().UnixMicro()+int64(rand.Intn(1000)))
		tm.configurationLock.Unlock()
		tm.logger.Info("Start removing:" + s.NodeId + " (" + s.NodeIp + ":" + s.NodePort + ") - New partition table:" + strconv.FormatInt(tm.tempPartitionTable.Timestamp, 10))
	}

	if !abort {
		newServerList := server.InitServerList(&tm.tempPartitionTable)

		for _, server := range newServerList {
			if server.NodeId != tm.clusterManager.currentNode.GetId() {
				//send the new partition table to the server
				tm.configurationLock.Lock()
				tm.logger.Info("Sending partition table " + strconv.FormatInt(tm.tempPartitionTable.Timestamp, 10) + " to " + server.NodeId)
				err := tm.clusterManager.commManager.SendUpdatePartitionTableRequest(server.NodeId, &tm.tempPartitionTable)
				tm.configurationLock.Unlock()
				if err != nil {
					tm.logger.Error(err, "Could not send new partition table to "+server.NodeId+"("+server.NodeIp+":"+server.NodePort)

					//abort the process and send a message to the requestor saying that the process could not be completed
					abortMessage := "Could not send new partition table to " + server.NodeId + "(" + server.NodeIp + ":" + server.NodePort + "): " + err.Error()
					tm.configurationLock.Lock()
					sendTo := tm.requestsRouter[s.NodeId]
					tm.configurationLock.Unlock()
					tm.clusterManager.commManager.SendAbortPartitionTableChangesToRequestor(sendTo, abortMessage)
					abort = true
					break
				}
			}
		}

		if !abort {
			//save locally
			err := tm.clusterManager.updatePartitionTable(&tm.tempPartitionTable)
			if err != nil {
				//do not return any error to the cluster, its a problem with this node
				tm.logger.Error(err, "Error committing partition table")
			}

			//broadcast commit and release
			tm.broadcastCommit(newServerList)
		}
	}

	if abort {
		tm.broadcastReleaseMaster(s.NodeId)
	}
}

func (tm *TopologyManager) manageUpdatePartitionTableRequest(newPartitionTable *partitioner.PartitionTable) {
	//This request should only be received by the master node
	tm.configurationLock.Lock()
	tm.tempPartitionTable = *newPartitionTable
	tm.configurationLock.Unlock()
}

func (tm *TopologyManager) broadcastReleaseMaster(nodeRequestingId string) {
	servers := tm.clusterManager.GetServers()
	for _, server := range servers {
		if server.NodeId != tm.clusterManager.currentNode.GetId() && server.NodeId != nodeRequestingId {
			err := tm.clusterManager.commManager.SendReleaseMasterRequest(server.NodeId)
			if err != nil {
				tm.logger.Error(err, "Could not release master from "+server.NodeId+"("+server.NodeIp+":"+server.NodePort+")")
			}
		}
	}
}

func (tm *TopologyManager) broadcastCommit(newServerList map[string]server.Server) {
	for _, server := range newServerList {
		if server.NodeId != tm.clusterManager.currentNode.GetId() {
			tm.logger.Info("Sending commit partition table " + strconv.FormatInt(tm.tempPartitionTable.Timestamp, 10) + " to " + server.NodeId)
			err := tm.clusterManager.commManager.SendCommitPartitionTableRequest(server.NodeId)
			if err != nil {
				tm.logger.Error(err, "Could not commit new partition table to "+server.NodeId+"("+server.NodeIp+":"+server.NodePort+")")
			}
		}
	}
}

func (tm *TopologyManager) releaseMaster(requestedByNodeId string, force bool) {
	//This request should only be received by the master node
	tm.configurationLock.Lock()

	if tm.currentMasterNodeId == requestedByNodeId || force {
		tm.waitingForCommit = false
		tm.currentMasterNodeId = ""
		tm.tempPartitionTable = partitioner.PartitionTable{}
		if tm.waitForMasterRelease {
			tm.masterReleaseReqReceived <- true
		}
	}

	tm.configurationLock.Unlock()
}

func (tm *TopologyManager) manageCommitPartitionTableRequest(requestedByNodeId string) {
	err := tm.clusterManager.updatePartitionTable(&tm.tempPartitionTable)
	if err != nil {
		//do not return any error to the cluster, its a problem with this node

		tm.logger.Error(err, "Error committing partition table")

	} else {
		tm.logger.Info("Partition table committed, new timestamp: " + strconv.FormatInt(tm.tempPartitionTable.Timestamp, 10))
	}

	tm.releaseMaster(requestedByNodeId, false)
}

func (tm *TopologyManager) HandlePossibleMasterCrash(crashedNodeId string) {
	tm.configurationLock.Lock()
	if tm.currentMasterNodeId == crashedNodeId {
		tm.configurationLock.Unlock()
		tm.logger.Info("Current master node " + crashedNodeId + " crashed")
		tm.releaseMaster("", true)
	} else {
		tm.configurationLock.Unlock()
	}
}
