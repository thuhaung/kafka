# Broker Types and Configuration

## Metadata Model

Each node must have metadata with the following fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| ClusterID | UUID | Yes | Cluster identifier shared by all nodes in the same cluster |
| NodeID | int | Yes | Unique numeric node identifier |
| Role | role | Yes | `broker` or `controller` |
| LogDir | string | Yes | Local directory containing partition logs and the local copy of the cluster metadata log |
| ListenerConfig | listener | Yes | Single configured network listener for this node |
| SecurityProtocol | string | Yes | The configured listener alias must map to `PLAINTEXT` in this phase |
| ControllerQuorum | list of controller endpoints | Yes | Controller quorum endpoints used during broker and non-leader controller startup discovery |
| LeaderController | controller endpoint | No at initial parse, Yes after startup discovery for brokers and non-leader controllers | The resolved current controller leader stored in memory after discovery |

### Roles

Supported roles are:

- `broker`
- `controller`

A node may run as exactly one role in this phase:

- broker
- controller

### Listener Alias and Protocol

Each node configures exactly one listener in this phase. That listener also acts
as the advertised listener.

The listener name is an operator-chosen alias such as `BROKER`, `CLIENT`, or
`INTERNAL`.

Only one security protocol is supported in this phase:

- `PLAINTEXT`: used for broker-client, broker-broker, broker-controller, and
  controller-controller traffic

The configured listener alias must be mapped to `PLAINTEXT` through
`listener.security.protocol.map`.

### Listener Fields

Each listener configuration contains:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| Type | string | Yes | Listener alias, for example `BROKER` |
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
listeners=BROKER://localhost:9096
listener.security.protocol.map=BROKER:PLAINTEXT
controller.quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
log.dir=kraft-cluster/node3
```

## Field Mapping

The initial implementation should interpret the properties file as follows.

| Property | Maps To | Notes |
| --- | --- | --- |
| `cluster.id` | ClusterID | Persisted UUID for the cluster |
| `process.roles` | Role | Exactly one role in this phase |
| `node.id` | NodeID | Integer node identifier. `0` is valid |
| `listeners` | ListenerConfig | Declares the single local listener using one listener alias. It also acts as the advertised listener |
| `listener.security.protocol.map` | SecurityProtocol | Must map the configured listener alias to `PLAINTEXT` |
| `controller.quorum.voters` | ControllerQuorum | Brokers and non-leader controllers choose one controller from this set during startup |
| `log.dir` | LogDir | Single local directory in this phase |

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
	ClusterID           string
	NodeID              int
	Role                Role
	LogDir              string
	ListenerConfig      ListenerConfig
	ControllerQuorum    []ControllerEndpoint
	LeaderController    *ControllerEndpoint
	SecurityProtocol    string
}
```

Implementation notes:

- `ClusterID` should be parsed and validated as a UUID even if stored as a
  string in the struct
- `Role` must contain exactly one supported value
- `SecurityProtocol` defaults to `PLAINTEXT`
- `LogDir` is a single directory loaded from `log.dir` in this phase
- `ListenerConfig` is the node's single bind and advertised listener in this
  phase
- the listener alias may be any non-empty name supported by the parser, but its
  protocol map entry must resolve to `PLAINTEXT`
- `ControllerQuorum` should be parsed from `controller.quorum.voters` for both
  broker and controller roles
- `LeaderController` is not loaded from disk and is populated only after
  successful startup discovery for broker-role nodes and non-leader controller
  nodes

## Validation Rules

The startup loader should enforce these rules:

- `node.id` must parse as an integer, and `0` is valid
- `cluster.id` must be present and must parse as a UUID
- `process.roles` must contain exactly one supported role
- `listeners` must define exactly one supported listener
- `listener.security.protocol.map` must contain an entry for that listener alias
- that map entry must resolve to `PLAINTEXT`
- `controller.quorum.voters` must parse into one or more controller endpoints
  for broker and controller nodes
- `log.dir` must resolve to exactly one local directory in this phase
