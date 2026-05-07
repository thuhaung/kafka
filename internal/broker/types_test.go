package broker

import "testing"

func TestBrokerTypeConstantsMatchDesignValues(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "broker role", got: string(RoleBroker), want: "broker"},
		{name: "controller role", got: string(RoleController), want: "controller"},
		{name: "plaintext listener", got: string(ListenerPLAINTEXT), want: "PLAINTEXT"},
		{name: "controller listener", got: string(ListenerController), want: "CONTROLLER"},
		{name: "plaintext security protocol", got: string(SecurityProtocolPLAINTEXT), want: "PLAINTEXT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("got %q, want %q", test.got, test.want)
			}
		})
	}
}

func TestNodeConfigCanRepresentBrokerNodeMetadata(t *testing.T) {
	leader := &ControllerEndpoint{NodeID: 1, Host: "localhost", Port: 9093}

	config := NodeConfig{
		ClusterID: "4f3e7f6e-7c46-4f9f-b0db-2dcf6d3c6c14",
		NodeID:    3,
		Role:      RoleBroker,
		LogDir:    "kraft-cluster/node3",
		ListenerConfigs: []ListenerConfig{
			{Type: ListenerPLAINTEXT, Host: "localhost", Port: 9096},
			{Type: ListenerController, Host: "localhost", Port: 9097},
		},
		ControllerQuorum: []ControllerEndpoint{
			{NodeID: 1, Host: "localhost", Port: 9093},
			{NodeID: 2, Host: "localhost", Port: 9094},
			{NodeID: 3, Host: "localhost", Port: 9095},
		},
		LeaderController:    leader,
		SecurityProtocol:    SecurityProtocolPLAINTEXT,
		AdvertisedListeners: []ListenerConfig{{Type: ListenerPLAINTEXT, Host: "localhost", Port: 9096}},
		InterBrokerListener: ListenerPLAINTEXT,
		ControllerListener:  ListenerController,
	}

	if config.Role != RoleBroker {
		t.Fatalf("role = %q, want %q", config.Role, RoleBroker)
	}
	if len(config.ListenerConfigs) != 2 {
		t.Fatalf("listener count = %d, want 2", len(config.ListenerConfigs))
	}
	if config.LeaderController == nil || *config.LeaderController != *leader {
		t.Fatalf("leader controller = %#v, want %#v", config.LeaderController, leader)
	}
}

func TestLeaderPartitionStateCanIndexPartitionsByTopicAndID(t *testing.T) {
	key := PartitionKey{TopicName: "orders", PartitionID: 0}
	state := LeaderPartitionState{
		BrokerID: 3,
		Partitions: map[PartitionKey]PartitionMetadata{
			key: {
				TopicName:        "orders",
				PartitionID:      0,
				LeaderBrokerID:   3,
				ReplicaBrokerIDs: []int{3, 4, 5},
				ISRBrokerIDs:     []int{3, 4},
			},
		},
	}

	partition, ok := state.Partitions[key]
	if !ok {
		t.Fatalf("partition %v was not indexed", key)
	}
	if partition.LeaderBrokerID != state.BrokerID {
		t.Fatalf("leader broker = %d, want %d", partition.LeaderBrokerID, state.BrokerID)
	}
}
