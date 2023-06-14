package stream

type StreamEntry struct {
	Key   []byte
	Value []byte
}

type StreamMessage struct {
	TaskId         string
	CollectionName string
	Progress       uint64
	Data           []StreamEntry
}
