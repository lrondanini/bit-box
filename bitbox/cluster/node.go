package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/lrondanini/bit-box/bitbox/cluster/actions"
	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/stream"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/storage"

	"github.com/hashicorp/serf/serf"
)

type Node struct {
	id              string
	NodeIp          string
	NodePort        string
	clusterManager  *ClusterManager
	heartbitManager *HeartbitManager
	logger          *utils.InternalLogger
	storageManager  *storage.StorageManager
	nodeStats       *NodeStats
}

func GenerateNodeId(nodeIp string, nodePort string) string {
	return nodeIp + ":" + nodePort
}

func InitNode(conf utils.Configuration) (*Node, error) {
	utils.VerifyAndSetConfiguration(&conf)

	var node Node = Node{
		id:             GenerateNodeId(conf.NODE_IP, conf.NODE_PORT),
		NodeIp:         conf.NODE_IP,
		NodePort:       conf.NODE_PORT,
		logger:         utils.GetLogger(),
		storageManager: storage.InitStorageManager(),
	}

	cm, err := InitClusterManager(&node)
	if err != nil {
		return nil, err
	}

	node.clusterManager = cm

	node.heartbitManager = InitHeartbitManager(node.id, conf.NODE_HEARTBIT_PORT)

	var ns *NodeStats
	ns, err = InitNodeStats()
	if err != nil {
		return nil, err
	}

	node.nodeStats = ns

	return &node, nil
}

func (n *Node) GetId() string {
	return n.id
}

func (n *Node) Start(ctx context.Context, signalChan chan os.Signal, forceRejoin bool, onReadyChan chan bool) error {
	ch := n.clusterManager.StartCommunications()
	defer n.Shutdown()

	err := n.clusterManager.JoinCluster(forceRejoin)
	if err != nil {
		return err
	}

	n.clusterManager.NodeReadyForWork()

	if onReadyChan != nil {
		onReadyChan <- true
	}

	fmt.Println("Node started successfully")

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
					n.StartHeartbit()
					//n.startSyncNewPartitionTableProcess()
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
	n.storageManager.Shutdown()
	n.nodeStats.Shutdown()
	fmt.Println("...cya!")
}

func (n *Node) StartHeartbit() {
	if !n.heartbitManager.started && len(n.clusterManager.servers) > 1 {
		n.heartbitManager.JoinCluster()
		if n.heartbitManager.started {
			go n.manageHeartbitEvents()
		}
	}
}

func (n *Node) UpdateHeartbitPartitionTable(timestamp int64) {
	n.heartbitManager.SetPartitionTableTimestamp(timestamp)
}

func (n *Node) GetHeartbitStatus() []server.Server {
	return n.heartbitManager.GetServers()
}

func (n *Node) manageHeartbitEvents() {
	for {
		r := <-n.heartbitManager.eventChannel

		switch r.EventType() {
		case serf.EventMemberLeave:
			n.clusterManager.VerifyNodeCrash()
			n.clusterManager.UpdateServersHeartbitStatus()
		case serf.EventMemberJoin:
			n.clusterManager.UpdateServersHeartbitStatus()
		case serf.EventMemberUpdate:
			_, ok := r.(serf.MemberEvent)
			if !ok {
				continue
			}

			n.clusterManager.UpdateServersHeartbitStatus()

		}

		/*

			USEFUL NOTES: NOTE AS A QUERY CAN "ANSWER" WHILE USER EVENTS CAN ONLY RECEIVE
			REMINDER: queries are not propagate to a node that was not in the cluster when the query was sent

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

func (n *Node) Upsert(collectionName string, key interface{}, value interface{}) error {

	bKey, err := storage.ToBytes(key)

	if err != nil {
		return err
	}

	hash := partitioner.GetHash(bKey)

	//find WHERE to store this data
	isLocal := true

	if isLocal {
		c, e := n.storageManager.GetCollection(collectionName)

		if e != nil {
			return e
		}

		isNew, _ := c.Exists(key)

		v := storage.DbValue{
			Hash:  hash,
			Value: value,
		}

		c.Set(key, v)

		ev := Event{
			EventType:      UpsertEvent,
			CollectionName: collectionName,
			IsNew:          isNew,
		}
		n.nodeStats.Log(ev)
	}

	return nil
}

func (n *Node) upsertFromClusterStream(collectionName string, data []stream.StreamEntry) error {
	c, e := n.storageManager.GetCollection(collectionName)
	if e != nil {
		return e
	}

	return c.UpsertFromStreaming(data, func() {
		ev := Event{
			EventType:      UpsertEvent,
			CollectionName: collectionName,
			IsNew:          true,
		}
		n.nodeStats.Log(ev)
	})
}

func (n *Node) deleteForClusterSync(collectionName string, keys [][]byte) error {
	c, e := n.storageManager.GetCollection(collectionName)
	if e != nil {
		return e
	}

	return c.DeleteKeys(keys, func() {
		ev := Event{
			EventType:      DeleteEvent,
			CollectionName: collectionName,
		}
		n.nodeStats.Log(ev)
	})
}

// used in .Get to set the value of the pointer
func decAlloc(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	return v
}

func (n *Node) Get(collectionName string, key interface{}, value interface{}) error {
	vv := reflect.ValueOf(value)
	if vv.Type().Kind() != reflect.Pointer {
		return errors.New("attempt to decode into a non-pointer")
	}

	bKey, err := storage.ToBytes(key)

	if err != nil {
		return err
	}

	hash := partitioner.GetHash(bKey)

	//find WHERE to get this data
	isLocal := true

	if isLocal {
		c, e := n.storageManager.GetCollection(collectionName)

		if e != nil {
			return e
		}

		v := storage.DbValue{
			Hash:  hash,
			Value: value,
		}

		e = c.Get(key, &v)
		if e != nil {
			return e
		}

		decAlloc(vv).Set(reflect.ValueOf(v.Value))

		ev := Event{
			EventType:      ReadEvent,
			CollectionName: collectionName,
		}
		n.nodeStats.Log(ev)

	}

	return nil
}

func (n *Node) Delete(collectionName string, key interface{}) error {
	bKey, err := storage.ToBytes(key)

	if err != nil {
		return err
	}

	hash := partitioner.GetHash(bKey)

	//find WHERE to get this data
	isLocal := true

	if hash > 0 {
		isLocal = true
	}

	if isLocal {
		c, e := n.storageManager.GetCollection(collectionName)

		if e != nil {
			return e
		}

		ev := Event{
			EventType:      DeleteEvent,
			CollectionName: collectionName,
		}
		n.nodeStats.Log(ev)

		e = c.Delete(key)

		if e != nil {
			return e
		}
	}

	return nil
}

func (n *Node) GetIterator(collectionName string) (*storage.Iterator, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}

	return c.GetIterator()
}

func (n *Node) GetIteratorFrom(collectionName string, from interface{}) (*storage.Iterator, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}

	return c.GetIteratorFrom(from)
}

func (n *Node) GetFilteredIterator(collectionName string, from interface{}, to interface{}) (*storage.Iterator, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}

	return c.GetFilteredIterator(from, to)
}

func (n *Node) DeleteCollection(collectionName string) error {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return e
	}

	return c.DeleteCollection()
}

func (n *Node) GetStats() *NodeStats {
	return n.nodeStats
}
