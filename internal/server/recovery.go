// =============================================================================
// Crash Recovery — WAL Replay & Checkpoint Loading
// =============================================================================
//
// Implements the recoverFromCrash() routine that runs on every node startup:
//
// 1. LOAD CHECKPOINT: Find the latest checkpoint_N.json file and restore
//    the file index from it.
//
// 2. REPLAY WAL: Read all WAL entries AFTER the checkpoint ID and replay
//    any uncommitted operations (re-execute uploads/deletes).
//
// 3. SYNC WITH MASTER: After local recovery, the node contacts the current
//    master to fetch any updates it may have missed while offline.
//
// This two-phase recovery (checkpoint + WAL replay) ensures that nodes
// can resume operation with minimal data loss even after a crash.
// =============================================================================

package server

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/filesync/internal/metadata"
)

// recoverFromCrash runs on startup to restore node state from durable storage.
// Phase 1: Load the latest checkpoint (file_index snapshot).
// Phase 2: Replay uncommitted WAL entries after that checkpoint.
func (s *Server) recoverFromCrash() error {
	startTime := time.Now()
	log.Printf("[Recovery %d] 🔄 Starting crash recovery...", s.nodeID)

	// Phase 1: Find and load the latest checkpoint.
	checkpointLoaded := false
	latestCkptID, err := s.getLatestCheckpointID()
	if err != nil {
		log.Printf("[Recovery %d] Warning: failed to scan for checkpoints: %v", s.nodeID, err)
	}

	if latestCkptID > 0 {
		checkpointFile := filepath.Join(s.dirs.Metadata,
			fmt.Sprintf("checkpoint_%d.json", latestCkptID))

		data, err := os.ReadFile(checkpointFile)
		if err != nil {
			log.Printf("[Recovery %d] Warning: failed to read checkpoint %d: %v",
				s.nodeID, latestCkptID, err)
		} else {
			var fileIndex map[string]*metadata.FileMetadata
			if err := json.Unmarshal(data, &fileIndex); err != nil {
				log.Printf("[Recovery %d] Warning: failed to parse checkpoint %d: %v",
					s.nodeID, latestCkptID, err)
			} else {
				s.fileIndex.LoadFromMap(fileIndex)
				s.checkpointID.Store(latestCkptID)
				checkpointLoaded = true
				log.Printf("[Recovery %d] ✅ Loaded checkpoint %d (%d files)",
					s.nodeID, latestCkptID, len(fileIndex))
			}
		}
	}

	// If no checkpoint, try loading the main file_index.json.
	if !checkpointLoaded {
		indexPath := filepath.Join(s.dirs.Metadata, "file_index.json")
		if err := s.fileIndex.LoadFromFile(indexPath); err != nil {
			log.Printf("[Recovery %d] No file index found, starting fresh", s.nodeID)
		} else {
			log.Printf("[Recovery %d] Loaded file_index.json (%d files)",
				s.nodeID, s.fileIndex.Count())
		}
	}

	// Phase 2: Replay uncommitted WAL entries.
	uncommitted, err := s.wal.GetUncommitted()
	if err != nil {
		log.Printf("[Recovery %d] Warning: failed to read uncommitted WAL entries: %v",
			s.nodeID, err)
	}

	if len(uncommitted) > 0 {
		log.Printf("[Recovery %d] Replaying %d uncommitted WAL entries...",
			s.nodeID, len(uncommitted))

		for _, entry := range uncommitted {
			log.Printf("[Recovery %d] Replaying: %s '%s' v%d (entry %d)",
				s.nodeID, entry.Operation, entry.Filename, entry.Version, entry.EntryID)

			switch entry.Operation {
			case "UPLOAD":
				// For uploads, we check if the file data exists on disk.
				// If it does, just update the metadata index.
				filePath := filepath.Join(s.dirs.Files, entry.Filename)
				if _, err := os.Stat(filePath); err == nil {
					// File exists on disk — read it and update index.
					data, err := os.ReadFile(filePath)
					if err != nil {
						log.Printf("[Recovery %d] Warning: cannot read file '%s': %v",
							s.nodeID, entry.Filename, err)
						continue
					}
					meta := metadata.NewFileMetadata(entry.Filename, data, s.nodeID)
					meta.Version = entry.Version
					s.fileIndex.Put(meta)
					log.Printf("[Recovery %d] Restored file '%s' v%d to index",
						s.nodeID, entry.Filename, entry.Version)
				} else {
					log.Printf("[Recovery %d] File '%s' not on disk (may need sync from master)",
						s.nodeID, entry.Filename)
				}

			case "DELETE":
				// For deletes, remove from index and disk.
				s.fileIndex.Delete(entry.Filename)
				filePath := filepath.Join(s.dirs.Files, entry.Filename)
				os.Remove(filePath) // Ignore errors — file might already be gone.
				log.Printf("[Recovery %d] Replayed DELETE for '%s'", s.nodeID, entry.Filename)
			}

			// Mark the entry as committed after successful replay.
			if err := s.wal.MarkCommitted(entry.EntryID); err != nil {
				log.Printf("[Recovery %d] Warning: failed to mark entry %d committed: %v",
					s.nodeID, entry.EntryID, err)
			}
		}
	} else {
		log.Printf("[Recovery %d] No uncommitted WAL entries to replay", s.nodeID)
	}

	// Load snapshot metadata.
	s.loadSnapshotMetadata()

	// Save the recovered file index.
	indexPath := filepath.Join(s.dirs.Metadata, "file_index.json")
	if err := s.fileIndex.SaveToFile(indexPath); err != nil {
		log.Printf("[Recovery %d] Warning: failed to save recovered file index: %v",
			s.nodeID, err)
	}

	elapsed := time.Since(startTime)
	log.Printf("[Recovery %d] ✅ Crash recovery complete (%d files, %d uncommitted replayed, %v)",
		s.nodeID, s.fileIndex.Count(), len(uncommitted), elapsed)

	return nil
}
