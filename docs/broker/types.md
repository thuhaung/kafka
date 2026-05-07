# Broker Types and Configuration

## Metadata Model

Each node must have metadata with the following fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| ClusterID | UUID | Yes | Cluster identifier shared by all nodes in the same cluster |
| NodeID | int | Yes | Unique numeric node identifier |
| Role | role | Yes | `broker` or `controller` |
| LogDir | string | Yes | Local directory containing partition logs and the local copy of the cluster metadata log |
| ListenerConfigs | list of listeners | Yes | Configured network listeners for this node |
| SecurityProtocol | string | Yes | Defaults to `PLAINTEXT` in this phase |
| ControllerQuorum | list of controller endpoints | Yes | Controller quorum endpoints used during broker and non-leader controller startup discovery |
| LeaderController | controller endpoint | No at initial parse, Yes after startup discovery for brokers and non-leader controllers | The resolved current controller leader stored in memory after discovery |

### Roles

Supported roles are:

- `broker`
- `controller`

A node may run as exactly one role in this phase:

- broker
- controller

### Listener Types

Only two listener types are supported in this phase:

- `PLAINTEXT`: used for broker-client and broker-broker traffic
- `CONTROLLER`: used for broker-controller traffic

Custom listener names are not supported.

Because of that restriction, `ListenerConfigs` may contain at most:

- one `PLAINTEXT` listener
- one `CONTROLLER` listener

So the maximum number of configured listeners is `2`.

### Listener Fields

Each listener configuration contains:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| Type | string | Yes | `PLAINTEXT` or `CONTROLLER` |
| Host | string | Yes | Host or bind address |
| Port | int | Yes | TCP port |

## Server Start CLI and Properties Format

Each broker or controller is started through `kafka-server-start.sh`. The CLI
accepts a required `config.path` parameter that points to a properties file on
local disk:

```sh
kafka-server-start.sh config.path=/path/to/server.properties
```

The file may be named `node.properties`, `server.properties`, or any other
operator-chosen name. The startup contract is:

1. The process receives `config.path` before startup work begins
2. It reads the properties file at `config.path`
3. It parses the file once during startup
4. It stores the parsed result in memory for later use

If the node crashes and starts again, it reads the properties file from the
`config.path` provided to the new process. The parsed configuration is not
reloaded repeatedly during normal operation.

Example:

```properties
cluster.id=4f3e7f6e-7c46-4f9f-b0db-2dcf6d3c6c14
process.roles=broker
node.id=3
listeners=PLAINTEXT://localhost:9096,CONTROLLER://localhost:9097
advertised.listeners=PLAINTEXT://localhost:9096
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
controller.quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
log.dirs=kraft-cluster/node3
```

## Field Mapping

The initial implementation should interpret the properties file as follows.

| Property | Maps To | Notes |
| --- | --- | --- |
| `cluster.id` | ClusterID | Persisted UUID for the cluster |
| `process.roles` | Role | Exactly one role in this phase |
| `node.id` | NodeID | Integer node identifier |
| `listeners` | ListenerConfigs | Declares local listeners |
| `advertised.listeners` | advertised listener metadata | Only `PLAINTEXT` is expected in this phase |
| `listener.security.protocol.map` | SecurityProtocol per listener type | Must resolve both supported listeners to `PLAINTEXT` |
| `inter.broker.listener.name` | broker listener selection | Must be `PLAINTEXT` in this phase |
| `controller.listener.names` | controller listener selection | Must be `CONTROLLER` in this phase |
| `controller.quorum.voters` | ControllerQuorum | Brokers and non-leader controllers choose one controller from this set during startup |
| `log.dirs` | LogDir | Single local directory in this phase |

### Cluster ID

`ClusterID` is part of the broker metadata model, must be represented in memory
as a UUID, and is persisted under the `cluster.id` key.

## Proposed Go Types

The exact package path may evolve, but `internal/broker` should expose types
close to:

```go
package broker

type Role string

const (
	RoleBroker     Role = "broker"
	RoleController Role = "controller"
)

type ListenerType string

const (
	ListenerPLAINTEXT  ListenerType = "PLAINTEXT"
	ListenerController ListenerType = "CONTROLLER"
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
	SecurityProtocol    string
	AdvertisedListeners []ListenerConfig
	InterBrokerListener ListenerType
	ControllerListener  ListenerType
}
```

Implementation notes:

- `ClusterID` should be parsed and validated as a UUID even if stored as a
  string in the struct
- `Role` must contain exactly one supported value
- `SecurityProtocol` defaults to `PLAINTEXT`
- `LogDir` is a single directory in this phase even if the property name is
  `log.dirs`
- `ListenerConfigs` must contain no duplicate listener types
- `ControllerQuorum` should be parsed from `controller.quorum.voters` for both
  broker and controller roles
- `LeaderController` is not loaded from disk and is populated only after
  successful startup discovery for broker-role nodes and non-leader controller
  nodes
- `AdvertisedListeners` should be limited to the externally reachable broker
  listener metadata needed in this phase

## Validation Rules

The startup loader should enforce these rules:

- `node.id` must parse as an integer
- `cluster.id` must be present and must parse as a UUID
- `process.roles` must contain exactly one supported role
- `listeners` must define only supported listener types
- `listeners` must contain at most one `PLAINTEXT` listener
- `listeners` must contain at most one `CONTROLLER` listener
- `listener.security.protocol.map` must resolve configured listener types to
  `PLAINTEXT`
- `inter.broker.listener.name` must be `PLAINTEXT`
- `controller.listener.names` must be `CONTROLLER`
- `controller.quorum.voters` must parse into one or more controller endpoints
  for broker and controller nodes
- `log.dirs` must resolve to exactly one local directory in this phase
- if the node role is `broker`, both `PLAINTEXT` and `CONTROLLER` listeners
  must be configured
- if the node role is `controller`, a `CONTROLLER` listener must be configured
