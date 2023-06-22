

# bit-box

<img src="graffitiLogo.png" alt="bit-box" width="600"/>


Bit-box is an embeddable distributed key-value store based on Dynamo/Bigtable architectures designed to uniformally distribute tasks over a cluster of nodes.


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

To start a node as go routine:

```
var conf utils.Configuration
bitbox, err := bitbox.Init(conf)
onReadyChan := make(chan bool)
go bitBox.Start(false, onReadyChan)
<-onReadyChan
```

## Configuration

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

## Usage

### Collections

Bit-box store data in collections. A collection is automatically created on the first insert. Data in a collection are sorted according to the type of the key. For example, a key of type string follows a lexicographic order while a
key of type int follows ASC order. 
### Setting and getting key/values

To store a value:

```
bitbox.Set(collectionName string, key interface{}, value interface{}) error
```

To retrieve a value:

```
bitbox.Get(collectionName string, key interface{}, value interface{}) error
```

Note that you need to pass **value** as an pointer address so that bit-box can concert from byte array (stored in the db) to your specifc type. For example:

```
bitbox.Set("tasks", "one", "first-task") 

value := ""
_ := bitbox.Get("tasks", "one", &value)
fmt.Println(value) //will print first-task
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