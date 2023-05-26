package cluster

import (
	"strconv"
	"time"

	"github.com/lrondanini/bit-box/bitbox/cluster/server"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/serverStatus/heartBitStatus"

	"github.com/hashicorp/serf/serf"
)

type HeartbitManager struct {
	port         string
	serf         *serf.Serf
	eventChannel chan serf.Event
	started      bool
	tags         map[string]string
}

func InitHeartbitManager(nodeId string, port string) *HeartbitManager {

	var err error

	conf := serf.DefaultConfig()
	conf.NodeName = nodeId

	conf.MemberlistConfig.BindPort, err = strconv.Atoi(port)
	if err != nil {
		panic(err)
	}

	// nullLogger := log.New(ioutil.Discard, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	// conf.Logger = nullLogger
	// conf.MemberlistConfig.Logger = conf.Logger

	serfLogger := &utils.SerfLogWriter{}
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

func (h *HeartbitManager) JoinCluster(partitionTableTimestamp int64) {

	logger := utils.GetLogger()

	var conf = utils.GetClusterConfiguration()

	var ok = true
	if conf.CLUSTER_NODE_IP == "" || conf.CLUSTER_NODE_HEARTBIT_PORT == "" {
		ok = false
		logger.Error().Msg("Cannot start heartbit, please verify configuration for clusterNodeIp, clusterNodePort, clusterNodeHeartbitPort and restart this node")
	}

	if ok {

		_, err := h.serf.Join([]string{conf.CLUSTER_NODE_IP + ":" + conf.CLUSTER_NODE_HEARTBIT_PORT}, false)

		if err != nil {
			logger.Error().Msg("Cannot start heartbit: " + err.Error())
		}

		h.SetPartitionTableTimestamp(partitionTableTimestamp)
		h.UpdateHardwareStats()
		h.started = true

		go h.monitorHardwareStats()
	}

}

func (h *HeartbitManager) SetPartitionTableTimestamp(partitionTableTimestamp int64) {
	h.tags["ptTimestamp"] = strconv.FormatInt(partitionTableTimestamp, 10)
	h.setTags()
}

func (h *HeartbitManager) UpdateHardwareStats() {
	stats := utils.GetHardwareStats()
	h.tags["memory"] = stats.Memory
	h.tags["cpu"] = stats.CPU
	h.setTags()
}

func (h *HeartbitManager) setTags() {
	logger := utils.GetLogger()
	err := h.serf.SetTags(h.tags)
	if err != nil {
		logger.Error().Msg("Cannot set partition table timestamp for heartbit: " + err.Error())
	}
}

func (h *HeartbitManager) GetServers() []server.Server {
	var res []server.Server
	for _, m := range h.serf.Members() {
		res = append(res, server.Server{
			NodeId:           m.Name,
			NodeIp:           m.Addr.String(),
			NodeHeartbitPort: strconv.FormatUint(uint64(m.Port), 10),
			HeartbitStatus:   heartBitStatus.HeartBitStatus(m.Status),
			PartitionTable:   m.Tags["ptTimestamp"],
			Memory:           m.Tags["memory"],
			CPU:              m.Tags["cpu"],
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
