package metadata

func getPartitionsByTopic(image *Image, topicName string) []PartitionMetadata {
	var partitions []PartitionMetadata
	for key,_ := range image.Partitions {
		if key.TopicName == topicName {
			partitions = append(partitions, image.Partitions[key])
		}
	}
	return partitions
}
