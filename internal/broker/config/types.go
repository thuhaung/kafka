package config

type Role string

const (
	RoleBroker     Role = "broker"
	RoleController Role = "controller"
)

type SecurityProtocol string

const (
	PlaintextProtocol SecurityProtocol = "PLAINTEXT"
)

type ListenerConfig struct {
	Type string
	Host string
	Port int
}

type ControllerEndpoint struct {
	NodeID int
	Host   string
	Port   int
}

type NodeConfig struct {
	ClusterID                string
	NodeID                   int
	Role                     Role
	IsLeader                 bool
	LogDir                   string
	ListenerConfigs          []ListenerConfig
	ControllerQuorum         []ControllerEndpoint
	LeaderController         *ControllerEndpoint
	SecurityProtocol         SecurityProtocol
	ListenerSecurityProtocol map[string]SecurityProtocol
}
