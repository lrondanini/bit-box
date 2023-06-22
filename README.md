
<img src="graffitiLogo.png" alt="bit-box" width="600"/>


Bit-box is an embeddable distributed key-value store based on Dynamo/Bigtable architectures.

Born as part of another project, its main goal is to uniformally distribute tasks over a cluster of nodes.

### Features

- Embeddable First: although you'll find a standalone implementation here, bit-box was designed to be easilly embedded into your code   
- Distributed Footprint: no master node, data is uniformally distributed
- Elastic Scalability: adding/removing nodes is extremelly simple with no down time. 
- Fault Tolerance: data is automatically replicated among the nodes. If a node goes down, its load/tasks will be automatically managed by another node
- Event Streaming: you can subscribe to specific events in the cluster
- Scheduler: crontab like features to manage your task
- Atomicity: bit-box guarantees that a task will be performed only once 
- Monitoring: easy to maintain and monitor using our cli

### Can I use bit-box as a database?

Yes but bit-box does not implements any query language and it does not offer indexing features (beside the one on the primary key, of course). 

## Getting Started

```
go get github.com/lrondanini/bit-box/bitbox
```

To start a node:

```
var conf utils.Configuration
bitbox, err := bitbox.Init(conf)
bitBox.Start(false, nil)
```

To start a node as a routine:

```
var conf utils.Configuration
bitbox, err := bitbox.Init(conf)
onReadyChan := make(chan bool)
go bitBox.Start(false, onReadyChan)
<-onReadyChan
```

For configuration details click [here](#configuration)


## Usage

### Collections

Bit-box stores data in collections. A collection is automatically created on the first insert. Data in a collection are sorted according to the type of the key. For example, a key of type string follows a lexicographic order while a
key of type int is ASC ordered. Keys os type struct will be converted to its []byte form and ordered lexicographically.
### Setting, getting and deleting key/values

To store a value:

```
bitbox.Set(collectionName string, key interface{}, value interface{}) error
```

To retrieve a value:

```
bitbox.Get(collectionName string, key interface{}, value interface{}) error
```

_Note_ that you need to pass **value** as an pointer address so that bit-box can concert from byte array (stored in the db) to your specifc type. For example:

```
bitbox.Set("tasks", "one", "first-task") 

value := ""
_ := bitbox.Get("tasks", "one", &value)
fmt.Println(value) //will print "first-task"
```

To delete:

```
bitbox.Delete(collectionName string, key interface{}) error
```

### Subscribe to events

You can listen for events on a specific collection using:

```
eventsChannel := bitBox.SubscribeTo("collection-name")
for {
  event := <-eventsChannel

}
```

the event:

```
type Event struct {
	Type       EventType
	Collection string
	Key        []byte
}
```

EventType is an enum: Insert, Update, Delete


### Scanning

You can scan the content of a collection but only for the local node. To scan other nodes you'll need to use bit-box's TCP protocol and connect to the remote node.

To get an iterator you can use one of the following:

```
bitbox.GetLocalIterator(collectionName string) (*storage.Iterator, error) 

bitbox.GetLocalIteratorFrom(collectionName string, from interface{}) (*storage.Iterator, error)

bitbox.GetLocalFilteredIterator(collectionName string, from interface{}, to interface{}) (*storage.Iterator, error)
```

Using an iterator is extremelly simple:

```
it, _ := bitbox.GetLocalIterator(collectionName) 
for it.HasMore() {
  var value string
  var key int
  err := it.Next(&key, &value)
  fmt.Println(k, "=", v)
}
```

## Configuration <a name="configuration"></a>

Bit-box configuration is extremelly simple:

```
type Configuration struct {
	NODE_IP            string
	NODE_PORT          string
	NODE_HEARTBIT_PORT string
	NUMB_VNODES        int
	DATA_FOLDER        string

	CLUSTER_NODE_IP            string
	CLUSTER_NODE_PORT          string
	CLUSTER_NODE_HEARTBIT_PORT string

	LOGGER              Logger
	LOG_GOSSIP_PROTOCOL bool
	LOG_STORAGE         bool
}
```

Bit-box needs 2 ports. NODE_PORT is used for inter-node communication. This port is also used by any external client. NODE_HEARTBIT_PORT is used by the raft protocol.

DATA_FOLDER is where bit-box will store its data.

You can use NUMB_VNODES to manage the load of a specific node. See architecture notes for more details [here](#v-nodes).

CLUSTER_NODE_IP, CLUSTER_NODE_PORT and CLUSTER_NODE_HEARTBIT_PORT are used on bootstrap to connect to the cluster.

You can turn on/off logs for the storage and the raft protocol using LOG_GOSSIP_PROTOCOL and LOG_STORAGE

LOGGER must implement the following interface:

```
func (c *Logger) Trace(msg string) {...}
func (c *Logger) Debug(msg string) {...}
func (c *Logger) Info(msg string) {...}
func (c *Logger) Warn(msg string) {...}
func (c *Logger) Error(err error, msg string) {...}
func (c *Logger) Fatal(err error, msg string) {...}
func (c *Logger) Panic(err error, msg string) {...}
```

# Architecture

## <a name="v-nodes"></a> Vnodes

<!-- 
<-onReadyChan
ev := bitBox.SubscribeTo("dogs")
fmt.Println("Subscribed to dogs")
for {
  e := <-ev
  fmt.Println(e.ToString())
}
-->