# FileSync

FileSync is a distributed file-sharing and replicated storage system built in Go. It runs across multiple independent nodes on a local area network, coordinating file operations through gRPC. The system handles leader election, distributed mutual exclusion, asynchronous replication, coordinated snapshotting, and crash recovery.

## Architecture

The cluster consists of up to 4 nodes, each running an identical binary with a unique configuration. Every node stores its own copy of files, metadata, and a write-ahead log. One node is elected as the master to coordinate heartbeats and elections, but all nodes can independently execute write operations after acquiring distributed permission from peers. The system tolerates the failure of one node without losing data, provided files have been replicated.

Communication between nodes uses gRPC over Protocol Buffers. The service definition lives in `proto/filesync.proto`, and the generated Go bindings are in `gen/filesync/`.

## Algorithms

### Leader Election (Bully Algorithm)

The cluster uses the Bully algorithm to elect a master node. Each node is assigned a random priority at startup. When a node detects that the master is unreachable (no heartbeat received within the configured timeout, typically 300ms), it initiates an election by sending an ELECTION message to all other nodes. If a node with a higher priority receives this message, it responds and takes over the election process. The node with the highest priority that doesn't receive any response from a higher-priority node declares itself the new master and broadcasts a COORDINATOR message to all peers. The master then begins sending periodic heartbeats to maintain its leadership. If a master node exits or becomes unreachable, followers detect the absence of heartbeats and restart the election process.

### Mutual Exclusion (Ricart-Agrawala Algorithm)

Write operations (uploads and deletes) are protected by the Ricart-Agrawala distributed mutual exclusion algorithm. When a node wants to perform a write, it increments its Lamport logical clock, records a timestamp, and multicasts a REQUEST message containing that timestamp and its node ID to every other node in the cluster. Each receiving node decides whether to reply immediately or defer its reply. A node replies immediately if it is not interested in the critical section, or if the incoming request has higher priority (lower timestamp wins; ties are broken by lower node ID). If the receiving node is currently inside the critical section or is itself requesting with higher priority, it defers its reply until it exits. The requesting node enters the critical section only after receiving replies from all peers. Upon exiting, it sends all deferred replies. Unreachable peers are treated as implicit replies, since a crashed node cannot contest the critical section.

### Replication

After a write operation completes, the writing node asynchronously replicates the change to follower nodes. For uploads, the file data and metadata are streamed to peers using the ReplicateFile RPC. For deletes, a delete marker is sent so followers can remove their local copy. The number of replicas is controlled by the `replication_factor` setting in the configuration file. Replication does not block the client response.

### Coordinated Checkpointing (Koo-Toueg Inspired)

The system performs coordinated checkpoints after a configurable number of write operations (set by `checkpoint_interval`). When the threshold is reached, the initiating node assigns a globally unique checkpoint ID and broadcasts a CHECKPOINT-REQUEST to all peers. Each node flushes its in-memory file index to disk as a checkpoint file, syncs the write-ahead log, and replies with an acknowledgment. Once all nodes have acknowledged, the checkpoint is marked as globally consistent. Older write-ahead log entries from previous checkpoints can then be safely pruned. This ensures that all nodes share a consistent recovery point.

### Crash Recovery

On startup, each node performs a two-phase recovery. First, it looks for the latest checkpoint file and restores the file index from that snapshot. Second, it replays any uncommitted entries in the write-ahead log that were recorded after the checkpoint. This guarantees that all operations that were committed before a crash are recovered, and partially applied operations are either completed or rolled back.

### Snapshots

Snapshots provide a read-only, point-in-time view of the file index. They are coordinated by the master, which assigns a snapshot ID, broadcasts a SNAPSHOT-INIT request to all followers, and waits for acknowledgments. Each node saves an immutable copy of its current file index along with the WAL position at the time of the snapshot. Snapshots can be listed and read back through the CLI client.

## Project Structure

```
FileSync/
  cmd/
    filesync/       Server binary entry point
    client/         CLI client for interacting with the cluster
  internal/
    server/
      server.go             Node initialization and gRPC server setup
      election.go           Bully algorithm and heartbeat monitoring
      ricart_agrawala.go    Ricart-Agrawala mutual exclusion state machine
      fileops.go            Upload, Download, Delete, ListFiles handlers
      replication.go        Async file replication to followers
      checkpoint.go         Coordinated checkpointing protocol
      snapshot.go           Snapshot creation and management
      recovery.go           Crash recovery from WAL and checkpoints
      masterlog.go          Master election log recording
  gen/filesync/             Generated gRPC and Protobuf Go bindings
  proto/                    Protocol Buffers service definition
  config.yaml               Node configuration file
```

## Configuration

Each node requires a `config.yaml` file. Key settings:

| Parameter | Description | Default |
|---|---|---|
| `node_id` | Unique node identifier (1-4) | 1 |
| `listen_port` | gRPC port the node listens on | 50051 |
| `data_dir` | Base directory for local storage | `./data/node1` |
| `heartbeat_interval_ms` | How often the master sends heartbeats | 200 |
| `heartbeat_timeout_ms` | Heartbeat timeout before triggering election | 300 |
| `election_timeout_ms` | Bully election response timeout | 200 |
| `replication_factor` | Number of follower replicas per file | 2 |
| `checkpoint_interval` | Write operations before triggering a checkpoint | 500 |
| `peers` | List of other nodes (id and address) | -- |

## Running

Start a node:

```
go run cmd/filesync/main.go -config config.yaml
```

Use the CLI client to interact with the cluster:

```
go run cmd/client/main.go -server localhost:50051
```

Supported client commands: `upload`, `download`, `delete`, `list`, `snapshot`, `snapshots`, `leader`.

## Building

```
go build ./...
```

Requires Go 1.21 or later.
