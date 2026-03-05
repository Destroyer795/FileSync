// =============================================================================
// Package wal — Write-Ahead Log for FileSync
// =============================================================================
//
// Implements a durable, append-only Write-Ahead Log stored as JSONL
// (one JSON object per line) in operations.jsonl. Every mutating operation
// (UPLOAD, DELETE) is appended to the WAL BEFORE the operation is executed,
// ensuring crash recovery can replay uncommitted operations.
//
// Key operations:
//   - Append:    write a new entry before executing the operation
//   - MarkCommitted: mark an entry as committed after successful execution
//   - Replay:    load all entries after a given checkpoint ID for recovery
//   - Prune:     remove old entries that have been checkpointed
//
// Thread-safety: All operations are protected by a sync.Mutex.
// =============================================================================

package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// WALEntry represents a single entry in the Write-Ahead Log.
// This is the Go struct equivalent of WALEntryProto in the proto file.
type WALEntry struct {
	// The type of operation: "UPLOAD" or "DELETE".
	Operation string `json:"operation"`
	// The filename affected by this operation.
	Filename string `json:"filename"`
	// The file version at the time of this operation.
	Version int64 `json:"version"`
	// Unix timestamp (nanoseconds) when the WAL entry was created.
	Timestamp int64 `json:"timestamp"`
	// Whether this operation has been fully committed (persisted + replicated).
	// Uncommitted entries are replayed during crash recovery.
	Committed bool `json:"committed"`
	// Unique, monotonically increasing entry ID for ordering and deduplication.
	EntryID int64 `json:"entry_id"`
	// The checkpoint ID this entry belongs to (0 if not yet checkpointed).
	// Entries with checkpoint_id <= latest checkpoint can be pruned.
	CheckpointID int64 `json:"checkpoint_id"`
}

// WAL manages the durable write-ahead log file (operations.jsonl).
// All public methods are thread-safe.
type WAL struct {
	mu       sync.Mutex
	filePath string   // Full path to operations.jsonl
	file     *os.File // Open file handle for appending
	nextID   atomic.Int64
}

// NewWAL creates a new WAL instance, opening or creating operations.jsonl
// in the specified directory. Recovers the next entry ID from existing entries.
func NewWAL(walDir string) (*WAL, error) {
	filePath := filepath.Join(walDir, "operations.jsonl")

	// Open the WAL file for append (create if not exists).
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %s: %w", filePath, err)
	}

	w := &WAL{
		filePath: filePath,
		file:     file,
	}

	// Recover the highest entry ID from existing WAL entries so that
	// new entries get monotonically increasing IDs even after restarts.
	maxID, err := w.recoverMaxEntryID()
	if err != nil {
		log.Printf("[WAL] Warning: failed to recover max entry ID: %v", err)
		// Continue with 0 — not fatal.
	}
	w.nextID.Store(maxID + 1)

	log.Printf("[WAL] Initialized at %s, next entry ID: %d", filePath, w.nextID.Load())
	return w, nil
}

// Append writes a new WAL entry BEFORE the operation is executed.
// Returns the assigned entry ID. The entry is initially uncommitted.
func (w *WAL) Append(operation, filename string, version int64) (int64, error) {
	start := time.Now()
	log.Printf("[MutEx WAL] Acquiring WAL mutex for Append (op=%s file=%s)", operation, filename)
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for Append")
	}()

	entryID := w.nextID.Add(1) - 1

	entry := WALEntry{
		Operation: operation,
		Filename:  filename,
		Version:   version,
		Timestamp: time.Now().UnixNano(),
		Committed: false,
		EntryID:   entryID,
	}

	// Serialize to JSON and append as a single line.
	data, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// Write the JSON line + newline.
	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return 0, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// fsync to ensure durability — the WAL contract requires this.
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to fsync WAL: %w", err)
	}

	elapsed := time.Since(start)
	log.Printf("[WAL] Appended entry %d: op=%s file=%s ver=%d (took %v)",
		entryID, operation, filename, version, elapsed)

	return entryID, nil
}

// MarkCommitted marks a WAL entry as committed by rewriting the WAL.
// This is called after the operation has been successfully executed and replicated.
func (w *WAL) MarkCommitted(entryID int64) error {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for MarkCommitted (entry %d)", entryID)
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for MarkCommitted")
	}()

	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return err
	}

	// Find and mark the entry.
	found := false
	for i := range entries {
		if entries[i].EntryID == entryID {
			entries[i].Committed = true
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("WAL entry %d not found", entryID)
	}

	// Rewrite the entire WAL file with the updated entries.
	return w.rewriteLocked(entries)
}

// SetCheckpointID updates all entries up to the given entry ID with the
// specified checkpoint ID. Called during coordinated checkpointing.
func (w *WAL) SetCheckpointID(upToEntryID, checkpointID int64) error {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for SetCheckpointID")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for SetCheckpointID")
	}()

	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return err
	}

	for i := range entries {
		if entries[i].EntryID <= upToEntryID {
			entries[i].CheckpointID = checkpointID
		}
	}

	return w.rewriteLocked(entries)
}

// Replay returns all WAL entries with entry IDs strictly greater than
// afterEntryID. Used during crash recovery to replay uncommitted operations.
func (w *WAL) Replay(afterEntryID int64) ([]WALEntry, error) {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for Replay")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for Replay")
	}()

	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return nil, err
	}

	var result []WALEntry
	for _, entry := range entries {
		if entry.EntryID > afterEntryID {
			result = append(result, entry)
		}
	}

	log.Printf("[WAL] Replay: found %d entries after entry ID %d", len(result), afterEntryID)
	return result, nil
}

// GetUncommitted returns all WAL entries that have not been committed.
// Used during crash recovery to identify operations that need to be re-executed.
func (w *WAL) GetUncommitted() ([]WALEntry, error) {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for GetUncommitted")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for GetUncommitted")
	}()

	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return nil, err
	}

	var result []WALEntry
	for _, entry := range entries {
		if !entry.Committed {
			result = append(result, entry)
		}
	}

	return result, nil
}

// Prune removes all WAL entries with checkpoint_id <= the given checkpoint ID.
// Called after a successful coordinated checkpoint to free disk space.
func (w *WAL) Prune(checkpointID int64) error {
	start := time.Now()
	log.Printf("[MutEx WAL] Acquiring WAL mutex for Prune")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for Prune")
	}()

	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return err
	}

	// Keep only entries that are NOT covered by this checkpoint.
	var kept []WALEntry
	pruned := 0
	for _, entry := range entries {
		if entry.CheckpointID > 0 && entry.CheckpointID <= checkpointID {
			pruned++
		} else {
			kept = append(kept, entry)
		}
	}

	if err := w.rewriteLocked(kept); err != nil {
		return err
	}

	log.Printf("[WAL] Pruned %d entries (checkpoint <= %d), %d remaining (took %v)",
		pruned, checkpointID, len(kept), time.Since(start))
	return nil
}

// GetLatestEntryID returns the highest entry ID currently in the WAL.
// Returns 0 if the WAL is empty.
func (w *WAL) GetLatestEntryID() (int64, error) {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for GetLatestEntryID")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for GetLatestEntryID")
	}()

	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return 0, err
	}

	var maxID int64
	for _, entry := range entries {
		if entry.EntryID > maxID {
			maxID = entry.EntryID
		}
	}
	return maxID, nil
}

// Fsync flushes the WAL file to disk. Called during checkpointing.
func (w *WAL) Fsync() error {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for Fsync")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for Fsync")
	}()
	return w.file.Sync()
}

// Close closes the WAL file handle.
func (w *WAL) Close() error {
	log.Printf("[MutEx WAL] Acquiring WAL mutex for Close")
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		log.Printf("[MutEx WAL] Released WAL mutex for Close")
	}()
	return w.file.Close()
}

// ----------- Internal helpers -----------

// readAllEntriesLocked reads all entries from the WAL file.
// MUST be called with w.mu held.
func (w *WAL) readAllEntriesLocked() ([]WALEntry, error) {
	// Open a separate read handle (the main handle is append-only).
	readFile, err := os.Open(w.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL for reading: %w", err)
	}
	defer readFile.Close()

	var entries []WALEntry
	scanner := bufio.NewScanner(readFile)
	// Increase buffer size for very long lines (large metadata).
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines.
		}
		var entry WALEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("[WAL] Warning: skipping corrupt entry at line %d: %v", lineNum, err)
			continue
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan WAL: %w", err)
	}

	return entries, nil
}

// rewriteLocked rewrites the entire WAL file with the given entries.
// Uses atomic rename for crash safety. MUST be called with w.mu held.
func (w *WAL) rewriteLocked(entries []WALEntry) error {
	// Close the current file handle before rewriting.
	w.file.Close()

	// Write to a temp file first.
	tmpPath := w.filePath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp WAL: %w", err)
	}

	writer := bufio.NewWriter(tmpFile)
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to marshal WAL entry: %w", err)
		}
		writer.Write(data)
		writer.WriteByte('\n')
	}
	writer.Flush()
	tmpFile.Sync()
	tmpFile.Close()

	// Atomic rename.
	if err := os.Rename(tmpPath, w.filePath); err != nil {
		return fmt.Errorf("failed to rename temp WAL: %w", err)
	}

	// Reopen the file for appending.
	w.file, err = os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL: %w", err)
	}

	return nil
}

// recoverMaxEntryID scans the WAL to find the highest entry ID.
func (w *WAL) recoverMaxEntryID() (int64, error) {
	entries, err := w.readAllEntriesLocked()
	if err != nil {
		return 0, err
	}
	var maxID int64
	for _, entry := range entries {
		if entry.EntryID > maxID {
			maxID = entry.EntryID
		}
	}
	return maxID, nil
}
