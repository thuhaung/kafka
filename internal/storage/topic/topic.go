package topic

type TopicConfig struct {
	Name string
	Partitions int32
	ReplicationFactor int16
	RetentionMs int64
	RetentionBytes int64
	SegmentMs int64
	SegmentBytes int64
	MinInSyncReplicas int16
}
