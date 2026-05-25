package config

import "time"

/* --------- Network configs --------- */
const MAX_CONNECTIONS = 10

const MAX_WORKER_POOL_SIZE = 10

const API_TIMEOUT = 10 * time.Second


/* --------- Kafka Raft default ---------*/
const CLUSTER_METADATA_TOPIC = "__cluster_metadata"

const CLUSTER_METADATA_TOPIC_PARTITION = 0
