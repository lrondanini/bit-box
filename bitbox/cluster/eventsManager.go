package cluster

import (
	"fmt"
	"strconv"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/communication"
	"github.com/lrondanini/bit-box/bitbox/communication/tcp"
	"github.com/lrondanini/bit-box/bitbox/partitioner"

	"github.com/lrondanini/bit-box/bitbox/actions"
)

func CreateAckFrame(nodeId string) *tcp.Frame {
	sb, _ := communication.SerialiazeBody("ack")
	return &tcp.Frame{
		FromNodeId:     nodeId,
		MessageType:    tcp.Response,
		StreamId:       0,
		StreamPosition: 0,
		Action:         actions.NoAction,
		Error:          false,
		Body:           sb,
	}
}

type EventsManager struct {
	nodeId         string
	ackFrame       *tcp.Frame //to avoid having to calculate this value every time
	clusterManager *ClusterManager
	logger         *utils.Logger
}

func initEventsManager(nodeId string, clusterManager *ClusterManager) *EventsManager {
	logger := utils.GetLogger()
	var eventsManager EventsManager = EventsManager{}
	eventsManager.nodeId = nodeId
	eventsManager.clusterManager = clusterManager
	eventsManager.ackFrame = CreateAckFrame(nodeId)
	eventsManager.logger = logger
	return &eventsManager
}

func (em *EventsManager) HandleEvent(msg *tcp.MessageFromCluster) error {
	f := msg.Frame
	switch f.Action {
	case actions.Ping:
		em.onPing(&f, msg.ReplyToChannel)
	case actions.JoinClusterRequest:
		em.onJoinClusterRequest(&f, msg.ReplyToChannel)
	case actions.RequestToBecomeMaster:
		em.onRequestToBecomeMaster(&f, msg.ReplyToChannel)
	case actions.AbortPartitionTableChanges:
		em.onAbortPartitionTableChanges(&f, msg.ReplyToChannel)
	case actions.GetPartitionTableRequest:
		em.onGetPartitionTableRequest(&f, msg.ReplyToChannel)
	case actions.UpdatePartitionTableRequest:
		em.onUpdatePartitionTableRequest(&f, msg.ReplyToChannel)
	case actions.ReleaseMasterRequest:
		em.onReleaseMasterRequest(&f, msg.ReplyToChannel)
	case actions.ForceReleaseMasterRequest:
		em.onForceReleaseMasterRequest(&f, msg.ReplyToChannel)
	case actions.CommitPartitionTableRequest:
		em.onCommitPartitionTableRequest(&f, msg.ReplyToChannel)
	case actions.DecommissionNodeRequest:
		em.onDecommissionNodeRequest(&f, msg.ReplyToChannel)
	case actions.NodeBackOnlineNotification:
		em.onNodeBackOnlineNotification(&f, msg.ReplyToChannel)
	case actions.ClusterStatusRequest:
		em.onClusterStatusRequest(&f, msg.ReplyToChannel)

	}

	return nil
}

func (em *EventsManager) newErrorFrame(action actions.Action, errorMessage string) *tcp.Frame {
	sb, _ := communication.SerialiazeBody(errorMessage)
	return &tcp.Frame{
		FromNodeId:     em.nodeId,
		MessageType:    tcp.Response,
		StreamId:       0,
		StreamPosition: 0,
		Action:         action,
		Error:          true,
		Body:           sb,
	}
}

func (em *EventsManager) newFrame(action actions.Action, body string) *tcp.Frame {
	return &tcp.Frame{
		FromNodeId:     em.nodeId,
		MessageType:    tcp.Response,
		StreamId:       0,
		StreamPosition: 0,
		Action:         action,
		Error:          false,
		Body:           body,
	}
}

func (em *EventsManager) onPing(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	em.logger.Info().Msg("Ping from: " + f.FromNodeId)
	//debug.PrintStack()
	//fmt.Print("\n\n\n")
	replyToChannel <- *em.ackFrame
}

func (em *EventsManager) onNodeBackOnlineNotification(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	em.logger.Info().Msg("Node back online: " + f.FromNodeId)
	resBody, _ := communication.SerialiazeBody(em.clusterManager.partitionTable.Timestamp)
	replyToChannel <- *em.newFrame(actions.NoAction, resBody)
}

func (em *EventsManager) onJoinClusterRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {

	req := make(map[string]string)

	err := communication.DeserializeBody(f.Body, &req)
	if err != nil {
		replyToChannel <- *em.newErrorFrame(actions.NoAction, "Could not parse request: "+err.Error())
	} else {
		err = em.clusterManager.StartAddNewNode(f.FromNodeId, req["nodeId"], req["nodeIp"], req["nodePort"])
		if err != nil {
			replyToChannel <- *em.newErrorFrame(actions.NoAction, "Could not start adding new node: "+err.Error())
		} else {
			replyToChannel <- *em.ackFrame
		}
	}
}

func (em *EventsManager) onDecommissionNodeRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	req := make(map[string]string)
	err := communication.DeserializeBody(f.Body, &req)
	fmt.Println(req)
	if err != nil {
		replyToChannel <- *em.newErrorFrame(actions.NoAction, "Could not parse request: "+err.Error())
	} else {
		err = em.clusterManager.StartDecommissionNode(f.FromNodeId, req["nodeId"])
		if err != nil {
			replyToChannel <- *em.newErrorFrame(actions.NoAction, "Could not start removing node: "+err.Error())
		} else {
			replyToChannel <- *em.ackFrame
		}
	}
}

func (em *EventsManager) onRequestToBecomeMaster(f *tcp.Frame, replyToChannel chan tcp.Frame) {

	accepted, ptTimestamp := em.clusterManager.SetNewClusterMaster(f.FromNodeId)

	resBody, _ := communication.SerialiazeBody(ptTimestamp)
	if accepted {
		replyToChannel <- *em.newFrame(actions.MasterRequestAccepted, resBody)
	} else {
		replyToChannel <- *em.newErrorFrame(actions.MasterRequestRejected, resBody)
	}
}

func (em *EventsManager) onAbortPartitionTableChanges(f *tcp.Frame, replyToChannel chan tcp.Frame) {

	var message string

	err := communication.DeserializeBody(f.Body, &message)

	if err != nil {
		replyToChannel <- *em.newErrorFrame(actions.NoAction, "Could not parse request: "+err.Error())
	} else {
		em.clusterManager.manageAbortPartitionTableChangesRequest(message)
	}

	replyToChannel <- *em.ackFrame
}

func (em *EventsManager) onGetPartitionTableRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	em.logger.Info().Msg("Current partition table requested by: " + f.FromNodeId)
	resBody, _ := communication.SerialiazeBody(em.clusterManager.partitionTable)
	replyToChannel <- *em.newFrame(actions.NoAction, resBody)
}

func (em *EventsManager) onUpdatePartitionTableRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {

	var reqBody partitioner.PartitionTable
	err := communication.DeserializeBody(f.Body, &reqBody)
	if err != nil {
		replyToChannel <- *em.newErrorFrame(actions.NoAction, "Could not parse request: "+err.Error())
	} else {
		em.logger.Info().Msg("New partition table from " + f.FromNodeId + " with timestamp " + strconv.FormatInt(reqBody.Timestamp, 10))
		em.clusterManager.manageUpdatePartitionTableRequest(&reqBody)
		replyToChannel <- *em.ackFrame
	}
}

func (em *EventsManager) onReleaseMasterRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	em.clusterManager.manageReleaseMasterRequest(f.FromNodeId)
	replyToChannel <- *em.ackFrame
}

func (em *EventsManager) onForceReleaseMasterRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	em.clusterManager.manageForceReleaseMasterRequest(f.FromNodeId)
	replyToChannel <- *em.ackFrame
}

// this will also release the master
func (em *EventsManager) onCommitPartitionTableRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	em.clusterManager.manageCommitPartitionTableRequest(f.FromNodeId)
	replyToChannel <- *em.ackFrame
}

func (em *EventsManager) onClusterStatusRequest(f *tcp.Frame, replyToChannel chan tcp.Frame) {
	servers := em.clusterManager.GetClusterStatus()
	resBody, _ := communication.SerialiazeBody(servers)
	replyToChannel <- *em.newFrame(actions.NoAction, resBody)
}
