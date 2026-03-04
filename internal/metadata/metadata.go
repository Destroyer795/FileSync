// =============================================================================
// Package metadata — File Metadata & Snapshot Info for FileSync
// =============================================================================
//
// Defines the core data structures for tracking file state across the
// distributed cluster. These structs are serialized to JSON for on-disk
// persistence (file_index.json, checkpoint files, snapshot files).
//
// Key structs:
//   - FileMetadata:  tracks a single file's version, size, checksum, replicas
//   - SnapshotInfo:  immutable point-in-time record of the file index
//   - FileIndex:     the complete in-memory map of filename → FileMetadata
// =============================================================================

package metadata

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// FileMetadata tracks all essential metadata for a single file in the
// distributed file index. This is the Go struct equivalent of the
// FileMetadataProto protobuf message.
type FileMetadata struct {
	// The unique filename (primary key in the file index).
	Filename string `json:"filename"`
	// Monotonically increasing version number. Starts at 1, incremented
	// on each re-upload of the same filename.
	Version int64 `json:"version"`
	// File size in bytes.
	Size int64 `json:"size"`
	// SHA-256 hex digest of the file content for integrity verification.
	Checksum string `json:"checksum"`
	// List of node IDs that hold replicas of this file.
	Replicas []int32 `json:"replicas"`
	// Unix timestamp (nanoseconds) when this file was first uploaded.
	CreatedAt int64 `json:"created_at"`
	// Unix timestamp (nanoseconds) of the most recent modification.
	UpdatedAt int64 `json:"updated_at"`
}

// SnapshotInfo is the immutable metadata record for a cluster-wide snapshot.
// Each snapshot captures the complete file index state at a point in time.
type SnapshotInfo struct {
	// Globally unique snapshot identifier (e.g., "snap_1709654400_1").
	SnapshotID string `json:"snapshot_id"`
	// Optional human-readable label (e.g., "pre-migration").
	Label string `json:"label"`
	// Unix timestamp (nanoseconds) when this snapshot was created.
	CreatedAt int64 `json:"created_at"`
	// The node ID that initiated the snapshot.
	InitiatedBy int32 `json:"initiated_by"`
	// The WAL entry ID at snapshot time. Recovery replays entries after this.
	WALPosition int64 `json:"wal_position"`
	// Number of files in the index at snapshot time.
	FileCount int32 `json:"file_count"`
	// Total size of all files at snapshot time (bytes).
	TotalSizeBytes int64 `json:"total_size_bytes"`
	// The master's election term at snapshot time.
	Term int64 `json:"term"`
	// Node IDs that successfully participated in this snapshot.
	ParticipatingNodes []int32 `json:"participating_nodes"`
}

// FileIndex is the thread-safe in-memory file index.
// Maps filename → FileMetadata. Protected by a sync.RWMutex for
// concurrent read access (downloads/lists) with exclusive write access.
type FileIndex struct {
	mu    sync.RWMutex
	files map[string]*FileMetadata
}

// NewFileIndex creates an empty FileIndex.
func NewFileIndex() *FileIndex {
	return &FileIndex{
		files: make(map[string]*FileMetadata),
	}
}

// Get retrieves metadata for a file by name. Returns nil if not found.
// Thread-safe for concurrent reads.
func (fi *FileIndex) Get(filename string) *FileMetadata {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	meta, ok := fi.files[filename]
	if !ok {
		return nil
	}
	// Return a copy to prevent external mutation.
	copied := *meta
	copied.Replicas = make([]int32, len(meta.Replicas))
	copy(copied.Replicas, meta.Replicas)
	return &copied
}

// Put adds or updates a file's metadata in the index.
func (fi *FileIndex) Put(meta *FileMetadata) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	// Store a copy so callers can't mutate the stored value.
	stored := *meta
	stored.Replicas = make([]int32, len(meta.Replicas))
	copy(stored.Replicas, meta.Replicas)
	fi.files[stored.Filename] = &stored
}

// Delete removes a file from the index. Returns true if it existed.
func (fi *FileIndex) Delete(filename string) bool {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	_, ok := fi.files[filename]
	if ok {
		delete(fi.files, filename)
	}
	return ok
}

// List returns a copy of all file metadata entries.
func (fi *FileIndex) List() []*FileMetadata {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	result := make([]*FileMetadata, 0, len(fi.files))
	for _, meta := range fi.files {
		copied := *meta
		copied.Replicas = make([]int32, len(meta.Replicas))
		copy(copied.Replicas, meta.Replicas)
		result = append(result, &copied)
	}
	return result
}

// Count returns the number of files in the index.
func (fi *FileIndex) Count() int {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	return len(fi.files)
}

// TotalSize returns the sum of all file sizes in the index.
func (fi *FileIndex) TotalSize() int64 {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	var total int64
	for _, meta := range fi.files {
		total += meta.Size
	}
	return total
}

// Snapshot returns an immutable deep copy of the current file index.
// Used during coordinated checkpointing and snapshot creation.
func (fi *FileIndex) Snapshot() map[string]*FileMetadata {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	snapshot := make(map[string]*FileMetadata, len(fi.files))
	for k, v := range fi.files {
		copied := *v
		copied.Replicas = make([]int32, len(v.Replicas))
		copy(copied.Replicas, v.Replicas)
		snapshot[k] = &copied
	}
	return snapshot
}

// LoadFromMap replaces the entire index with the given map.
// Used during crash recovery to restore from a checkpoint.
func (fi *FileIndex) LoadFromMap(m map[string]*FileMetadata) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.files = make(map[string]*FileMetadata, len(m))
	for k, v := range m {
		copied := *v
		copied.Replicas = make([]int32, len(v.Replicas))
		copy(copied.Replicas, v.Replicas)
		fi.files[k] = &copied
	}
}

// SaveToFile persists the file index to a JSON file atomically.
// Writes to a temp file first, then renames for crash safety.
func (fi *FileIndex) SaveToFile(path string) error {
	fi.mu.RLock()
	snapshot := fi.files
	fi.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal file index: %w", err)
	}

	// Write atomically: write to temp file, then rename.
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file index: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename temp file index: %w", err)
	}
	return nil
}

// LoadFromFile loads the file index from a JSON file on disk.
func (fi *FileIndex) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No index file yet — start fresh.
			return nil
		}
		return fmt.Errorf("failed to read file index: %w", err)
	}

	var loaded map[string]*FileMetadata
	if err := json.Unmarshal(data, &loaded); err != nil {
		return fmt.Errorf("failed to unmarshal file index: %w", err)
	}

	fi.LoadFromMap(loaded)
	return nil
}

// ComputeSHA256 computes the SHA-256 hex digest of the given data.
func ComputeSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// NewFileMetadata creates a FileMetadata entry for a newly uploaded file.
func NewFileMetadata(filename string, data []byte, nodeID int32) *FileMetadata {
	now := time.Now().UnixNano()
	return &FileMetadata{
		Filename:  filename,
		Version:   1,
		Size:      int64(len(data)),
		Checksum:  ComputeSHA256(data),
		Replicas:  []int32{nodeID},
		CreatedAt: now,
		UpdatedAt: now,
	}
}
