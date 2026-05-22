package partition

import "github.com/thuhaung/kafka/internal/storage/segment"

type Partition struct {
	TopicName string
	PartitionID int32
	PartitionName string
	LeaderBrokerNodeID int
	ReplicaBrokerNodeIDs []int
	ISRNodeIDs []int
	LogDir string
	ActiveSegment *segment.Segment
	InactiveSegments []*segment.Segment
}
