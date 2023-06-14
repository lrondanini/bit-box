package cluster

import (
	"strconv"
	"sync"
	"time"

	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/server/serverStatus/heartBitStatus"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"

	"github.com/hashicorp/serf/serf"
)

type HeartbitManager struct {
	port         string
	serf         *serf.Serf
	eventChannel chan serf.Event
	started      bool
	tags         map[string]string
	mu           sync.Mutex
}

func InitHeartbitManager(nodeId string, port string) *HeartbitManager {

	var err error

	conf := serf.DefaultConfig()
	conf.NodeName = nodeId

	conf.MemberlistConfig.BindPort, err = strconv.Atoi(port)
	if err != nil {
		panic(err)
	}

	var clusterConfig = utils.GetClusterConfiguration()

	serfLogger := &utils.SerfLogWriter{
		Skip: !clusterConfig.LOG_GOSSIP_PROTOCOL,
	}
	conf.LogOutput = serfLogger
	conf.MemberlistConfig.LogOutput = serfLogger

	eventCh := make(chan serf.Event, 4)
	conf.EventCh = eventCh

	serf, e := serf.Create(conf)
	if e != nil {
		panic(e)
	}

	return &HeartbitManager{
		port:         port,
		serf:         serf,
		eventChannel: eventCh,
		tags:         make(map[string]string),
	}
}

func (h *HeartbitManager) Shutdown() {
	h.serf.Leave()
	h.serf.Shutdown()
}

func (h *HeartbitManager) JoinCluster() {

	logger := utils.GetLogger()

	var conf = utils.GetClusterConfiguration()

	var ok = true
	if conf.CLUSTER_NODE_IP == "" || conf.CLUSTER_NODE_HEARTBIT_PORT == "" {
		ok = false
		logger.Error(nil, "Cannot start heartbit, please verify configuration for clusterNodeIp, clusterNodePort, clusterNodeHeartbitPort and restart this node")
	}

	if ok {

		_, err := h.serf.Join([]string{conf.CLUSTER_NODE_IP + ":" + conf.CLUSTER_NODE_HEARTBIT_PORT}, false)

		if err != nil {
			logger.Error(err, "Cannot start heartbit")
		}

		h.UpdateHardwareStats()
		h.started = true

		go h.monitorHardwareStats()
	}

}

func (h *HeartbitManager) SetPartitionTableTimestamp(partitionTableTimestamp int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tags["ptTimestamp"] = strconv.FormatInt(partitionTableTimestamp, 10)
	h.setTags()
}

func (h *HeartbitManager) UpdateHardwareStats() {
	h.mu.Lock()
	defer h.mu.Unlock()
	stats := utils.GetHardwareStats()
	h.tags["memory"] = stats.Memory
	h.tags["cpu"] = stats.CPU
	h.setTags()
}

func (h *HeartbitManager) setTags() {
	logger := utils.GetLogger()
	err := h.serf.SetTags(h.tags)
	if err != nil {
		logger.Error(err, "Cannot set partition table timestamp for heartbit")
	}
}

func (h *HeartbitManager) GetServers() []server.Server {
	var res []server.Server
	for _, m := range h.serf.Members() {
		ptTimestamp := int64(0)
		if m.Tags["ptTimestamp"] != "" {
			var err error
			ptTimestamp, err = strconv.ParseInt(m.Tags["ptTimestamp"], 10, 64)
			if err != nil {
				ptTimestamp = int64(0)
			}
		}
		res = append(res, server.Server{
			NodeId:                  m.Name,
			NodeIp:                  m.Addr.String(),
			NodeHeartbitPort:        strconv.FormatUint(uint64(m.Port), 10),
			HeartbitStatus:          heartBitStatus.HeartBitStatus(m.Status),
			PartitionTableTimestamp: ptTimestamp,
			Memory:                  m.Tags["memory"],
			CPU:                     m.Tags["cpu"],
		})
	}

	return res
}

func (h *HeartbitManager) monitorHardwareStats() {
	for {
		h.UpdateHardwareStats()
		time.Sleep(5 * time.Minute)
	}
}
