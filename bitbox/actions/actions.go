package actions

type Action uint8

const (
	Ping Action = iota
	NoAction
	JoinClusterRequest
	DecommissionNodeRequest
	RequestToBecomeMaster
	MasterRequestAccepted
	MasterRequestRejected
	GetPartitionTableRequest
	UpdatePartitionTableRequest
	ReleaseMasterRequest
	ForceReleaseMasterRequest
	CommitPartitionTableRequest //it also releases the master
	AbortPartitionTableChanges  //used to send messages to the requestor in case the op cannot be completed
	NodeBackOnlineNotification  //sent to notify a node that the sender is back online
	ClusterStatusRequest
)

// actions sent from the cluster to the current node
type NodeActions uint8

const (
	Shutdown NodeActions = iota
	NewPartitionTable
)
