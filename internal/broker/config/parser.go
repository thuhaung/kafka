package config

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/thuhaung/kafka/internal/storage"
)

var (
	ErrInvalidConfig = errors.New("Invalid config")
)

const (
	propertyClusterID               = "cluster.id"
	propertyProcessRoles            = "process.roles"
	propertyNodeID                  = "node.id"
	propertyListeners               = "listeners"
	propertyAdvertisedListeners     = "advertised.listeners"
	propertySecurityProtocolMap     = "listener.security.protocol.map"
	propertyInterBrokerListenerName = "inter.broker.listener.name"
	propertyControllerListenerNames = "controller.listener.names"
	propertyControllerQuorumVoters  = "controller.quorum.voters"
	propertyLogDirs                 = "log.dirs"
)

func ParseConfig(path string) (*NodeConfig, error) {
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
		line := scanner.Text()
		if len(line) == 0 || line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
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
		SecurityProtocol: PlaintextProtocol,
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
		case propertyNodeID:
			nodeID, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			config.NodeID = nodeID
		case propertyListeners:
			listeners, err := parseListeners(value)
			if err != nil {
				return nil, err
			}
			config.ListenerConfigs = listeners
		case propertyAdvertisedListeners:
			listeners, err := parseListeners(value)
			if err != nil {
				return nil, err
			}
			config.AdvertisedListeners = listeners
		case propertySecurityProtocolMap:
			protocol, err := parseSecurityProtocolMap(value)
			if err != nil {
				return nil, err
			}
			config.SecurityProtocol = protocol
		case propertyInterBrokerListenerName:
			listenerType, err := parseListenerType(value)
			if err != nil {
				return nil, err
			}
			config.InterBrokerListener = listenerType
		case propertyControllerListenerNames:
			listenerType, err := parseListenerType(value)
			if err != nil {
				return nil, err
			}
			config.ControllerListener = listenerType
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

func parseRole(value string) (Role, error) {
	role := Role(strings.TrimSpace(value))
	switch role {
	case RoleBroker, RoleController:
		return role, nil
	default:
		return "", fmt.Errorf("%w: unsupported process.roles value %q", ErrInvalidConfig, value)
	}
}

func parseListenerType(value string) (ListenerType, error) {
	listenerType := ListenerType(strings.TrimSpace(value))
	switch listenerType {
	case PlaintextListener, ControllerListener:
		return listenerType, nil
	default:
		return "", fmt.Errorf("%w: unsupported listener type %q", ErrInvalidConfig, value)
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

	listenerType, err := parseListenerType(parts[0])
	if err != nil {
		return ListenerConfig{}, err
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

func parseSecurityProtocolMap(value string) (SecurityProtocol, error) {
	parts := splitCSV(value)
	for _, part := range parts {
		mapping := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(mapping) != 2 {
			return "", fmt.Errorf("%w: invalid security protocol mapping %q", ErrInvalidConfig, part)
		}

		if strings.TrimSpace(mapping[1]) != string(PlaintextProtocol) {
			return "", fmt.Errorf("%w: unsupported security protocol %q", ErrInvalidConfig, mapping[1])
		}
	}

	return PlaintextProtocol, nil
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
