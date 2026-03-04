// =============================================================================
// Package config — YAML Configuration Loader for FileSync Nodes
// =============================================================================
//
// Reads config.yaml to bootstrap a FileSync node with its unique ID,
// gRPC listen port, peer addresses (other 3 laptops on the LAN), and
// local storage directory paths.
//
// The config file is designed to be easily editable for physical deployments
// across 4 separate laptops — just change the IP addresses in the peers list.
// =============================================================================

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// PeerConfig holds the connection details for a single remote peer node.
type PeerConfig struct {
	// Unique integer ID for this peer (1-4).
	ID int32 `yaml:"id"`
	// Network address in "ip:port" format, e.g. "192.168.1.102:50051".
	Address string `yaml:"address"`
}

// Config holds the complete configuration for a single FileSync node.
// This is loaded from config.yaml at startup.
type Config struct {
	// ----------- Node Identity -----------

	// Unique integer ID for this node (1-4). Higher IDs win Bully elections.
	NodeID int32 `yaml:"node_id"`
	// The gRPC port this node listens on, e.g. 50051.
	ListenPort int `yaml:"listen_port"`

	// ----------- Storage Paths -----------

	// Base directory for all node-local storage. Subdirectories (files/,
	// metadata/, snapshots/, wal/) are created automatically under this path.
	DataDir string `yaml:"data_dir"`

	// ----------- Cluster Peers -----------

	// List of the other 3 nodes in the cluster. Each entry has an ID and
	// an "ip:port" address. For local testing, use localhost with different ports.
	Peers []PeerConfig `yaml:"peers"`

	// ----------- Timing Parameters -----------

	// How often the master sends heartbeats to followers (milliseconds).
	// Default: 200ms.
	HeartbeatIntervalMs int `yaml:"heartbeat_interval_ms"`
	// If no heartbeat is received within this duration, trigger an election (ms).
	// Default: 300ms.
	HeartbeatTimeoutMs int `yaml:"heartbeat_timeout_ms"`
	// Timeout for waiting for OK responses during a Bully election (ms).
	// Default: 200ms.
	ElectionTimeoutMs int `yaml:"election_timeout_ms"`
	// Number of follower replicas to maintain for each file (1 or 2).
	// Default: 2.
	ReplicationFactor int `yaml:"replication_factor"`
	// Number of write operations before the master triggers a checkpoint.
	// Default: 500.
	CheckpointInterval int `yaml:"checkpoint_interval"`
}

// Derived directory paths computed from DataDir.
// These are NOT in the YAML — they are computed by InitDirectories().
type DirPaths struct {
	Files     string // DataDir/files/     — actual file data
	Metadata  string // DataDir/metadata/  — JSON indexes, checkpoints
	Snapshots string // DataDir/snapshots/ — immutable snapshot files
	WAL       string // DataDir/wal/       — operations.jsonl
}

// HeartbeatInterval returns the heartbeat interval as a time.Duration.
func (c *Config) HeartbeatInterval() time.Duration {
	return time.Duration(c.HeartbeatIntervalMs) * time.Millisecond
}

// HeartbeatTimeout returns the heartbeat timeout as a time.Duration.
func (c *Config) HeartbeatTimeout() time.Duration {
	return time.Duration(c.HeartbeatTimeoutMs) * time.Millisecond
}

// ElectionTimeout returns the election timeout as a time.Duration.
func (c *Config) ElectionTimeout() time.Duration {
	return time.Duration(c.ElectionTimeoutMs) * time.Millisecond
}

// LoadConfig reads and parses the YAML config file at the given path.
// Returns the parsed Config struct or an error if the file is missing/invalid.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	cfg := &Config{
		// Set sensible defaults — overridden by YAML values if present.
		HeartbeatIntervalMs: 200,
		HeartbeatTimeoutMs:  300,
		ElectionTimeoutMs:   200,
		ReplicationFactor:   2,
		CheckpointInterval:  500,
		DataDir:             "./data",
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Validate required fields.
	if cfg.NodeID < 1 || cfg.NodeID > 4 {
		return nil, fmt.Errorf("node_id must be between 1 and 4, got %d", cfg.NodeID)
	}
	if cfg.ListenPort <= 0 || cfg.ListenPort > 65535 {
		return nil, fmt.Errorf("listen_port must be between 1 and 65535, got %d", cfg.ListenPort)
	}
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("peers list must not be empty")
	}

	return cfg, nil
}

// InitDirectories creates all required local storage subdirectories
// under DataDir. Returns the computed DirPaths struct.
// Creates: files/, metadata/, snapshots/, wal/
func InitDirectories(dataDir string) (*DirPaths, error) {
	dirs := &DirPaths{
		Files:     filepath.Join(dataDir, "files"),
		Metadata:  filepath.Join(dataDir, "metadata"),
		Snapshots: filepath.Join(dataDir, "snapshots"),
		WAL:       filepath.Join(dataDir, "wal"),
	}

	// Create each directory with full parent chain.
	for _, dir := range []string{dirs.Files, dirs.Metadata, dirs.Snapshots, dirs.WAL} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return dirs, nil
}
