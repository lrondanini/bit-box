package communication

import (
	"errors"
	"strconv"

	"github.com/lrondanini/bit-box/bitbox/cluster/actions"
	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/stream"
	"github.com/lrondanini/bit-box/bitbox/communication/tcp"
	"github.com/lrondanini/bit-box/bitbox/communication/types"
)

func DeserializeBody(body string, toObject interface{}) error {
	return tcp.DecodeBody(body, toObject)
}

func SerialiazeBody(body interface{}) (string, error) {
	return tcp.EncodeBody(body)
}

type CommunicationManager struct {
	nodeId            string
	nodeIp            string
	nodePort          string
	ReceiverChannel   <-chan tcp.MessageFromCluster
	tcpServer         *tcp.TcpServer
	tcpClientsManager *tcp.TcpClientsManager
}

func StartCommManager(nodeId string, nodeIp string, nodePort string) *CommunicationManager {

	var commManager CommunicationManager = CommunicationManager{}
	commManager.nodeId = nodeId
	commManager.nodeIp = nodeIp
	commManager.nodePort = nodePort
	commManager.tcpServer = tcp.InitServer(nodeIp, nodePort)
	commManager.ReceiverChannel = commManager.tcpServer.Run()
	commManager.tcpClientsManager = tcp.InitTcpClientsManager(nodeId)

	return &commManager
}

func (c *CommunicationManager) Shutdown() {
	c.tcpServer.Shutdown()
}

func (c *CommunicationManager) SendPing(toNodeId string) (string, error) {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.Ping, "ping")

	if err != nil {
		return "", err
	}

	var replyStr string

	err = DeserializeBody(reply.Body, &replyStr)
	if err != nil {
		return "", err
	}

	return replyStr, nil
}

func (c *CommunicationManager) SendNodeBackOnlineNotification(toNodeId string) (int64, error) {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.NodeBackOnlineNotification, "")

	if err != nil {
		return 0, err
	}

	var remoteTimestamp int64
	DeserializeBody(reply.Body, &remoteTimestamp)

	return remoteTimestamp, nil
}

func (c *CommunicationManager) SendJoinClusterRequest(toNodeId string, nodeId string, nodeIp string, nodePort string, numbOfVNodes int) error {

	body := make(map[string]string)

	body["nodeId"] = nodeId
	body["nodeIp"] = nodeIp
	body["nodePort"] = nodePort
	body["numbOfVNodes"] = strconv.Itoa(numbOfVNodes)

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.JoinClusterRequest, body)

	if err != nil {
		return err
	}

	if reply.Error {
		var errMsg string
		DeserializeBody(reply.Body, &errMsg)
		return errors.New(errMsg)
	}

	return nil
}

// request to toNodeId decomission of a specific node (nodeId)
func (c *CommunicationManager) SendDecommissionNodeRequest(toNodeId string, nodeId string) error {

	body := make(map[string]string)

	body["nodeId"] = nodeId

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.DecommissionNodeRequest, body)

	if err != nil {
		return err
	}

	if reply.Error {
		var errMsg string
		DeserializeBody(reply.Body, &errMsg)
		return errors.New(errMsg)
	}

	return nil
}

func (c *CommunicationManager) SendRequestToBecomeMaster(toNodeId string) (bool, int64, error) {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.RequestToBecomeMaster, "")

	if err != nil {
		return false, 0, err
	}

	var remotePartitionTableTimestamp int64
	DeserializeBody(reply.Body, &remotePartitionTableTimestamp)

	if reply.Action == actions.MasterRequestAccepted {
		return true, remotePartitionTableTimestamp, nil
	} else if reply.Action == actions.MasterRequestRejected {
		return false, remotePartitionTableTimestamp, nil
	}

	return false, 0, nil
}

// sent to a node trying to join or leave (decommision) the cluster in case the op cannot be completed
func (c *CommunicationManager) SendAbortPartitionTableChangesToRequestor(toNodeId string, message string) error {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.AbortPartitionTableChanges, message)

	if err != nil {
		return err
	}

	var replyStr string

	err = DeserializeBody(reply.Body, &replyStr)
	if err != nil {
		return err
	}

	return nil
}

func (c *CommunicationManager) GetPartitionTableRequest(toNodeId string) (*partitioner.PartitionTable, error) {
	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.GetPartitionTableRequest, "")

	if err != nil {
		return nil, err
	}

	var newerPartitionTable partitioner.PartitionTable
	DeserializeBody(reply.Body, &newerPartitionTable)

	return &newerPartitionTable, nil
}

func (c *CommunicationManager) SendUpdatePartitionTableRequest(toNodeId string, partitionTable *partitioner.PartitionTable) error {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.UpdatePartitionTableRequest, partitionTable)

	if err != nil {
		return err
	}

	if reply.Error {
		errorMessage := ""
		err = DeserializeBody(reply.Body, &errorMessage)
		if err != nil {
			return err
		}
		return errors.New(errorMessage)
	}

	return nil
}

func (c *CommunicationManager) SendReleaseMasterRequest(toNodeId string) error {

	_, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.ReleaseMasterRequest, "")

	if err != nil {
		return err
	}
	return nil
}

func (c *CommunicationManager) SendForceReleaseMasterRequest(toNodeId string) error {

	_, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.ForceReleaseMasterRequest, "")

	if err != nil {
		return err
	}
	return nil
}

func (c *CommunicationManager) SendCommitPartitionTableRequest(toNodeId string) error {
	_, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.CommitPartitionTableRequest, "")

	if err != nil {
		return err
	}
	return nil
}

func (c *CommunicationManager) SendClusterStatusRequest(toNodeId string) ([]server.Server, error) {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.ClusterStatusRequest, "")

	if err != nil {
		return nil, err
	}

	var servers []server.Server
	DeserializeBody(reply.Body, &servers)

	return servers, err
}

func (c *CommunicationManager) SendGetNodeStatsRequest(toNodeId string) (types.NodeStatsResponse, error) {

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.GetNodeStatsRequest, "")

	if err != nil {
		return types.NodeStatsResponse{}, err
	}

	var stats types.NodeStatsResponse
	DeserializeBody(reply.Body, &stats)

	return stats, err
}

func (c *CommunicationManager) SendStartDataStreamRequest(taskId string, toNodeId string, from uint64, to uint64) error {

	req := types.DataSyncTaskRequest{
		TaskId: taskId,
		From:   from,
		To:     to,
	}

	reply, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.StartDataStreamRequest, req)

	if err != nil {
		return err
	}

	if reply.Error {
		var errMsg string
		DeserializeBody(reply.Body, &errMsg)
		return errors.New(errMsg)
	}

	return nil
}

func (c *CommunicationManager) SendDataStreamChunk(toNodeId string, data stream.StreamMessage) error {

	_, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.SendDataStreamChunk, data)

	if err != nil {
		return err
	}

	return nil
}

func (c *CommunicationManager) SendDataStreamTaskCompleted(toNodeId string, taskId string) error {

	_, err := c.tcpClientsManager.SendMessage(toNodeId, tcp.Request, actions.SendDataStreamTaskCompleted, taskId)

	if err != nil {
		return err
	}

	return nil
}
