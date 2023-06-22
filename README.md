

# bit-box

<img src="graffitiLogo.png" alt="bit-box" width="600"/>


Bit-box is an embeddable distributed key-value store based on Dynamo/Bigtable architectures designed to uniformally distribute tasks over a cluster of nodes.

see [vndoes](#v-nodes)

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

### Configuration

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

You can use NUMB_VNODES to manage the load of a specific node. 

## Architecture

### Vnodes (#v-nodes)

<!-- 
<-onReadyChan
ev := bitBox.SubscribeTo("dogs")
fmt.Println("Subscribed to dogs")
for {
  e := <-ev
  fmt.Println(e.ToString())
}
-->