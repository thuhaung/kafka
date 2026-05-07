package broker

// Role identifies the single runtime responsibility assigned to a node.
type Role string

const (
	RoleBroker     Role = "broker"
	RoleController Role = "controller"
)

// ListenerType identifies the supported network listener classes.
type ListenerType string

const (
	ListenerPLAINTEXT  ListenerType = "PLAINTEXT"
	ListenerController ListenerType = "CONTROLLER"
)

// SecurityProtocol identifies the transport security mode for a listener.
type SecurityProtocol string

const (
	SecurityProtocolPLAINTEXT SecurityProtocol = "PLAINTEXT"
)

// ListenerConfig describes a host and port bound or advertised by this node.
type ListenerConfig struct {
	Type ListenerType
	Host string
	Port int
}

// ControllerEndpoint identifies one configured controller quorum member.
type ControllerEndpoint struct {
	NodeID int
	Host   string
	Port   int
}

// NodeConfig is the validated static node metadata loaded at startup.
//
// LeaderController is runtime-only state discovered after startup for broker
// nodes. It is not loaded from node.properties.
type NodeConfig struct {
	ClusterID           string
	NodeID              int
	Role                Role
	LogDir              string
	ListenerConfigs     []ListenerConfig
	ControllerQuorum    []ControllerEndpoint
	LeaderController    *ControllerEndpoint
	SecurityProtocol    SecurityProtocol
	AdvertisedListeners []ListenerConfig
	InterBrokerListener ListenerType
	ControllerListener  ListenerType
}

// PartitionKey identifies one partition within the cluster.
type PartitionKey struct {
	TopicName   string
	PartitionID int32
}

// PartitionMetadata is the broker-local partition leadership metadata needed
// before produce, fetch, and replication APIs are introduced.
type PartitionMetadata struct {
	TopicName        string
	PartitionID      int32
	LeaderBrokerID   int
	ReplicaBrokerIDs []int
	ISRBrokerIDs     []int
}

// LeaderPartitionState is a derived, broker-local view of partitions currently
// led by BrokerID.
type LeaderPartitionState struct {
	BrokerID   int
	Partitions map[PartitionKey]PartitionMetadata
}
