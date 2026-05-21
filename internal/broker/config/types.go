package config

type Role string

const (
	RoleBroker     Role = "broker"
	RoleController Role = "controller"
)

type ListenerType string

const (
	PlaintextListener  ListenerType = "PLAINTEXT"
	ControllerListener ListenerType = "CONTROLLER"
)

type SecurityProtocol string

const (
	PlaintextProtocol SecurityProtocol = "PLAINTEXT"
)

type ListenerConfig struct {
	Type ListenerType
	Host string
	Port int
}

type ControllerEndpoint struct {
	NodeID int
	Host   string
	Port   int
}

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
