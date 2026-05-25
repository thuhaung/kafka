package config

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/thuhaung/kafka/internal/storage"
)

const (
	propertyClusterID              = "cluster.id"
	propertyProcessRoles           = "process.roles"
	propertyNodeID                 = "node.id"
	propertyListeners              = "listeners"
	propertySecurityProtocolMap    = "listener.security.protocol.map"
	propertyControllerQuorumVoters = "controller.quorum.voters"
	propertyLogDirs                = "log.dir"
	propertyIsLeader               = "is.leader"
)

var (
	ErrInvalidConfig              = errors.New("Invalid config")
	ErrInvalidClusterID           = errors.New("Invalid " + propertyClusterID)
	ErrInvalidLogDir              = errors.New("Invalid " + propertyLogDirs)
	ErrInvalidListenerProtocol    = errors.New("Invalid listener protocol. Only PLAINTEXT is supported at the moment")
	ErrInvalidRole                = errors.New("Invalid " + propertyProcessRoles)
	ErrEmptyListeners             = errors.New("Exactly one listener must be configured")
	ErrEmptyControllerQuorum      = errors.New("Controller quorum voters must be configured for controller role")
	ErrInvalidNodeID              = errors.New("Invalid " + propertyNodeID)
	ErrControllerNotFoundInQuorum = errors.New("Controller node ID must be included in controller quorum voters")
)

func ParseConfig(path string) (*NodeConfig, error) {
	log.Printf("Parsing broker config from %s", path)
	
	path, err := storage.ResolveFilePath(path)
	if err != nil {
		return nil, err
	}

	properties, err := parseProperties(path)
	if err != nil {
		return nil, err
	}

	config, err := buildNodeConfig(properties)
	if err != nil {
		return nil, err
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	resolveConfig(config)

	return config, nil
}

func parseProperties(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	defer file.Close()

	properties := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: malformed property line %q", ErrInvalidConfig, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		properties[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	return properties, nil
}

func buildNodeConfig(properties map[string]string) (*NodeConfig, error) {
	config := &NodeConfig{
		SecurityProtocol:         PlaintextProtocol,
		ListenerSecurityProtocol: make(map[string]SecurityProtocol),
	}

	for key, value := range properties {
		switch key {
		case propertyClusterID:
			config.ClusterID = value
		case propertyProcessRoles:
			role, err := parseRole(value)
			if err != nil {
				return nil, err
			}
			config.Role = role
		case propertyIsLeader:
			isLeader, err := parseIsLeader(value)
			if err != nil {
				return nil, err
			}
			config.IsLeader = isLeader
		case propertyNodeID:
			nodeID, err := parseNodeID(value)
			if err != nil {
				return nil, err
			}
			config.NodeID = nodeID
		case propertyListeners:
			listeners, err := parseListeners(value)
			if err != nil {
				return nil, err
			}
			if len(listeners) != 1 {
				return nil, ErrEmptyListeners
			}
			config.ListenerConfigs = listeners
		case propertySecurityProtocolMap:
			protocolMap, err := parseSecurityProtocolMap(value)
			if err != nil {
				return nil, err
			}
			config.ListenerSecurityProtocol = protocolMap
		case propertyControllerQuorumVoters:
			quorum, err := parseControllerQuorum(value)
			if err != nil {
				return nil, err
			}
			config.ControllerQuorum = quorum
		case propertyLogDirs:
			config.LogDir = value
		}
	}

	return config, nil
}

func parseNodeID(value string) (int, error) {
	nodeID, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidNodeID, err)
	}
	return nodeID, nil
}

func parseIsLeader(value string) (bool, error) {
	switch strings.TrimSpace(value) {
	case "true", "True", "TRUE":
		return true, nil
	case "false", "False", "FALSE":
		return false, nil
	default:
		return false, fmt.Errorf("%w: invalid boolean value %q for is.leader", ErrInvalidConfig, value)
	}
}

func parseRole(value string) (Role, error) {
	role := Role(strings.TrimSpace(value))
	switch role {
	case RoleBroker, RoleController:
		return role, nil
	default:
		return "", fmt.Errorf("%w: unsupported process.roles value %q", ErrInvalidConfig, value)
	}
}

func parseListeners(value string) ([]ListenerConfig, error) {
	parts := splitCSV(value)
	listeners := make([]ListenerConfig, 0, len(parts))
	for _, part := range parts {
		listener, err := parseListener(part)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}

func parseListener(value string) (ListenerConfig, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "://", 2)
	if len(parts) != 2 {
		return ListenerConfig{}, fmt.Errorf("%w: invalid listener %q", ErrInvalidConfig, value)
	}

	listenerType := strings.TrimSpace(parts[0])
	if listenerType == "" {
		return ListenerConfig{}, fmt.Errorf("%w: invalid listener alias %q", ErrInvalidConfig, value)
	}

	host, port, err := net.SplitHostPort(parts[1])
	if err != nil {
		return ListenerConfig{}, fmt.Errorf("%w: invalid listener address %q", ErrInvalidConfig, value)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		return ListenerConfig{}, fmt.Errorf("%w: invalid listener port %q", ErrInvalidConfig, value)
	}

	return ListenerConfig{
		Type: listenerType,
		Host: host,
		Port: portNum,
	}, nil
}

func parseControllerQuorum(value string) ([]ControllerEndpoint, error) {
	parts := splitCSV(value)
	quorum := make([]ControllerEndpoint, 0, len(parts))
	for _, part := range parts {
		endpoint, err := parseControllerEndpoint(part)
		if err != nil {
			return nil, err
		}
		quorum = append(quorum, endpoint)
	}
	return quorum, nil
}

func parseControllerEndpoint(value string) (ControllerEndpoint, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "@", 2)
	if len(parts) != 2 {
		return ControllerEndpoint{}, fmt.Errorf("%w: invalid controller quorum voter %q", ErrInvalidConfig, value)
	}

	nodeID, err := strconv.Atoi(parts[0])
	if err != nil {
		return ControllerEndpoint{}, fmt.Errorf("%w: invalid controller node id %q", ErrInvalidConfig, value)
	}

	host, port, err := net.SplitHostPort(parts[1])
	if err != nil {
		return ControllerEndpoint{}, fmt.Errorf("%w: invalid controller quorum address %q", ErrInvalidConfig, value)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		return ControllerEndpoint{}, fmt.Errorf("%w: invalid controller quorum port %q", ErrInvalidConfig, value)
	}

	return ControllerEndpoint{
		NodeID: nodeID,
		Host:   host,
		Port:   portNum,
	}, nil
}

func parseSecurityProtocolMap(value string) (map[string]SecurityProtocol, error) {
	parts := splitCSV(value)
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: empty security protocol map", ErrInvalidConfig)
	}

	protocols := make(map[string]SecurityProtocol, len(parts))
	for _, part := range parts {
		mapping := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(mapping) != 2 {
			return nil, fmt.Errorf("%w: invalid security protocol mapping %q", ErrInvalidConfig, part)
		}

		listenerAlias := strings.TrimSpace(mapping[0])
		if listenerAlias == "" {
			return nil, fmt.Errorf("%w: invalid listener alias in security protocol map %q", ErrInvalidConfig, part)
		}

		protocol := SecurityProtocol(strings.TrimSpace(mapping[1]))
		if protocol != PlaintextProtocol {
			return nil, fmt.Errorf("%w: unsupported security protocol %q", ErrInvalidConfig, mapping[1])
		}

		protocols[listenerAlias] = protocol
	}

	return protocols, nil
}

func splitCSV(value string) []string {
	rawParts := strings.Split(value, ",")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		parts = append(parts, trimmed)
	}
	return parts
}

func validateConfig(config *NodeConfig) error {
	if err := uuid.Validate(config.ClusterID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidClusterID, err)
	}

	if config.LogDir == "" {
		return ErrInvalidLogDir
	}
	if _, err := storage.CheckFolderExists(config.LogDir); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidLogDir, err)
	}

	if config.Role == "" {
		return ErrInvalidRole
	}
	if config.Role == RoleBroker && config.IsLeader {
		return ErrInvalidRole
	}

	if config.NodeID < 0 {
		return ErrInvalidNodeID
	}

	if len(config.ControllerQuorum) == 0 {
		return ErrEmptyControllerQuorum
	}

	if config.Role == RoleController {
		foundNode := false
		for _, endpoint := range config.ControllerQuorum {
			if endpoint.NodeID < 0 {
				return ErrInvalidNodeID
			}
			if endpoint.NodeID == config.NodeID {
				foundNode = true
			}
		}
		if !foundNode {
			return ErrControllerNotFoundInQuorum
		}
	}

	if len(config.ListenerConfigs) != 1 {
		return ErrEmptyListeners
	}

	listener := config.ListenerConfigs[0]
	if listener.Type == "" {
		return ErrEmptyListeners
	}
	if listener.Host == "" || listener.Port <= 0 {
		return fmt.Errorf("%w: listener host and port must be set", ErrInvalidConfig)
	}

	protocol, ok := config.ListenerSecurityProtocol[listener.Type]
	if !ok {
		return fmt.Errorf("%w: missing security protocol mapping for listener alias %q", ErrInvalidConfig, listener.Type)
	}
	if protocol != PlaintextProtocol {
		return ErrInvalidListenerProtocol
	}
	config.SecurityProtocol = protocol

	return nil
}

func resolveConfig(config *NodeConfig) {
	if config.IsLeader {
		for _, endpoint := range config.ControllerQuorum {
			if endpoint.NodeID == config.NodeID {
				config.LeaderController = &ControllerEndpoint{
					NodeID: endpoint.NodeID,
					Host:   endpoint.Host,
					Port:   endpoint.Port,
				}
				break
			}
		}
	}
}
