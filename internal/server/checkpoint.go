// =============================================================================
// Coordinated Checkpointing — Koo-Toueg Inspired Algorithm
// =============================================================================
//
// Implements a simplified Koo-Toueg coordinated checkpointing protocol:
//
// 1. TRIGGER: The master triggers a checkpoint every 500 write operations
//    (configurable via checkpoint_interval in config.yaml).
//
// 2. BROADCAST: The master assigns a globally unique checkpoint_id and
//    broadcasts CHECKPOINT-REQUEST to all follower nodes.
//
// 3. FOLLOWER RESPONSE: Each follower:
//    a) Flushes its in-memory file index to disk as checkpoint_N.json
//    b) Calls fsync() on the WAL to ensure all pending entries are durable
//    c) Replies with CHECKPOINT-DONE (CheckpointAck)
//
// 4. MASTER WAIT: The master BLOCKS until all 4 nodes (including itself)
//    have acknowledged the checkpoint. Once all ACKs are received, the
//    checkpoint is marked globally consistent and older WAL entries can
//    be safely pruned.
//
// This ensures that all nodes have a consistent view of the file index
// at the checkpoint boundary, enabling deterministic crash recovery.
// =============================================================================

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filesync/gen/filesync"
)

// triggerCheckpoint is called by the master when opsSinceCheckpoint reaches
// the configured checkpoint_interval. Coordinates a cluster-wide checkpoint.
func (s *Server) triggerCheckpoint() {
	if !s.isMasterNode() {
		return
	}

	startTime := time.Now()
	newCheckpointID := s.checkpointID.Add(1)
	currentTerm := s.term.Load()

	log.Printf("[Checkpoint %d] 🔄 Initiating checkpoint %d (term %d)",
		s.nodeID, newCheckpointID, currentTerm)

	// Reset the operation counter.
	s.opsSinceCheckpoint.Store(0)

	// Step 1: Perform the master's own local checkpoint first.
	walPos, err := s.performLocalCheckpoint(newCheckpointID)
	if err != nil {
		log.Printf("[Checkpoint %d] ❌ Master local checkpoint failed: %v", s.nodeID, err)
		return
	}
	log.Printf("[Checkpoint %d] Master local checkpoint done (WAL pos: %d)", s.nodeID, walPos)

	// Step 2: Broadcast CHECKPOINT-REQUEST to all followers.
	allPeers := s.getAllPeerIDs()
	ackCount := 1 // Master already acknowledged itself.
	totalNodes := len(allPeers) + 1

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peerID := range allPeers {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			peerStart := time.Now()
			client, err := s.getPeerClient(pid)
			if err != nil {
				log.Printf("[Checkpoint %d] Cannot reach peer %d: %v", s.nodeID, pid, err)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			ack, err := client.CheckpointRequest(ctx, &filesync.CheckpointReq{
				CheckpointId: newCheckpointID,
				MasterId:     s.nodeID,
				InitiatedAt:  startTime.UnixNano(),
				Term:         currentTerm,
			})

			if err != nil {
				log.Printf("[Checkpoint %d] Peer %d checkpoint failed: %v", s.nodeID, pid, err)
				return
			}

			peerElapsed := time.Since(peerStart)
			if ack.Success {
				mu.Lock()
				ackCount++
				mu.Unlock()
				log.Printf("[Checkpoint %d] Peer %d ACK (WAL pos: %d, took %v)",
					s.nodeID, pid, ack.WalPosition, peerElapsed)
			} else {
				log.Printf("[Checkpoint %d] Peer %d NACK: %s", s.nodeID, pid, ack.ErrorMessage)
			}
		}(peerID)
	}

	// Step 3: Block until all nodes have acknowledged.
	wg.Wait()

	totalElapsed := time.Since(startTime)

	if ackCount == totalNodes {
		log.Printf("[Checkpoint %d] ✅ Checkpoint %d globally consistent (%d/%d ACKs, %v)",
			s.nodeID, newCheckpointID, ackCount, totalNodes, totalElapsed)

		// Step 4: Update WAL entries with checkpoint ID and prune old entries.
		if err := s.wal.SetCheckpointID(walPos, newCheckpointID); err != nil {
			log.Printf("[Checkpoint %d] Warning: failed to update WAL checkpoint IDs: %v",
				s.nodeID, err)
		}

		// Prune WAL entries from PREVIOUS checkpoints (keep current).
		if newCheckpointID > 1 {
			if err := s.wal.Prune(newCheckpointID - 1); err != nil {
				log.Printf("[Checkpoint %d] Warning: failed to prune old WAL entries: %v",
					s.nodeID, err)
			}
		}
	} else {
		log.Printf("[Checkpoint %d] ⚠️ Checkpoint %d incomplete (%d/%d ACKs, %v)",
			s.nodeID, newCheckpointID, ackCount, totalNodes, totalElapsed)
	}
}

// performLocalCheckpoint flushes the file index to a checkpoint file
// and syncs the WAL. Returns the latest WAL entry ID at checkpoint time.
func (s *Server) performLocalCheckpoint(checkpointID int64) (int64, error) {
	// Get the current WAL position.
	walPos, err := s.wal.GetLatestEntryID()
	if err != nil {
		return 0, fmt.Errorf("failed to get WAL position: %w", err)
	}

	// Flush the file index to a checkpoint file.
	checkpointFile := filepath.Join(s.dirs.Metadata,
		fmt.Sprintf("checkpoint_%d.json", checkpointID))

	snapshot := s.fileIndex.Snapshot()
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	if err := os.WriteFile(checkpointFile, data, 0644); err != nil {
		return 0, fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	// Also update the main file_index.json.
	indexPath := filepath.Join(s.dirs.Metadata, "file_index.json")
	if err := s.fileIndex.SaveToFile(indexPath); err != nil {
		log.Printf("[Checkpoint %d] Warning: failed to save file_index.json: %v", s.nodeID, err)
	}

	// fsync the WAL to ensure all entries are durable.
	if err := s.wal.Fsync(); err != nil {
		return 0, fmt.Errorf("failed to fsync WAL: %w", err)
	}

	return walPos, nil
}

// ----------- CheckpointRequest Handler (Follower Side) -----------

// CheckpointRequest handles incoming checkpoint requests from the master.
// The follower flushes its file index and WAL, then responds with an ACK.
func (s *Server) CheckpointRequest(ctx context.Context, req *filesync.CheckpointReq) (*filesync.CheckpointAck, error) {
	startTime := time.Now()
	log.Printf("[Checkpoint %d] Received checkpoint request %d from master %d (term %d)",
		s.nodeID, req.CheckpointId, req.MasterId, req.Term)

	// Verify the request is from the current master with a valid term.
	if req.Term < s.term.Load() {
		return &filesync.CheckpointAck{
			NodeId:       s.nodeID,
			Success:      false,
			ErrorMessage: "Stale checkpoint request (old term)",
		}, nil
	}

	// Perform local checkpoint.
	walPos, err := s.performLocalCheckpoint(req.CheckpointId)
	if err != nil {
		log.Printf("[Checkpoint %d] Local checkpoint failed: %v", s.nodeID, err)
		return &filesync.CheckpointAck{
			NodeId:       s.nodeID,
			Success:      false,
			ErrorMessage: fmt.Sprintf("Checkpoint failed: %v", err),
		}, nil
	}

	// Update our checkpoint ID.
	s.checkpointID.Store(req.CheckpointId)

	elapsed := time.Since(startTime).Milliseconds()
	log.Printf("[Checkpoint %d] ✅ Checkpoint %d complete (WAL pos: %d, %dms)",
		s.nodeID, req.CheckpointId, walPos, elapsed)

	return &filesync.CheckpointAck{
		NodeId:             s.nodeID,
		Success:            true,
		WalPosition:        walPos,
		CheckpointDurationMs: elapsed,
	}, nil
}
