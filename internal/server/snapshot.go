// =============================================================================
// Filesystem Snapshots — Immutable Point-in-Time Copies
// =============================================================================
//
// Implements the coordinated snapshot protocol:
//
// 1. CREATE SNAPSHOT:
//    a) The master assigns a snapshot version/ID and broadcasts SNAPSHOT-INIT
//       to all follower nodes.
//    b) Each node atomically copies its file_index.json to
//       snapshots/snapshot_<id>.json and records the current WAL position.
//    c) Each node replies with SNAPSHOT-READY.
//    d) The master waits for 4/4 ACKs and commits the snapshot metadata.
//
// 2. LIST SNAPSHOTS:
//    Returns metadata for all available snapshots on this node.
//
// 3. READ SNAPSHOT:
//    Retrieves the immutable file index from a specific snapshot file.
//    This allows viewing historical file versions.
//
// Snapshots are IMMUTABLE once committed — they are never modified.
// =============================================================================

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/filesync/gen/filesync"
	"github.com/filesync/internal/metadata"
)

// ----------- CreateSnapshot Handler -----------

// CreateSnapshot coordinates an atomic snapshot across all 4 nodes.
// Only the master can initiate this. Broadcasts SnapshotInit to all followers.
func (s *Server) CreateSnapshot(ctx context.Context, req *filesync.CreateSnapshotRequest) (*filesync.CreateSnapshotResponse, error) {
	startTime := time.Now()

	if !s.isMasterNode() {
		// Forward to master.
		masterID := s.getMasterID()
		masterClient, err := s.getPeerClient(masterID)
		if err != nil {
			log.Printf("[Snapshot %d] ⚠️ Master %d unreachable during snapshot forward: %v. Initiating election.",
				s.nodeID, masterID, err)
			s.invalidatePeerConnection(masterID)
			s.masterID.Store(0)
			go s.startElection()
			return &filesync.CreateSnapshotResponse{
				Success: false,
				Message: fmt.Sprintf("Master %d unreachable, election triggered: %v", masterID, err),
			}, nil
		}
		resp, err := masterClient.CreateSnapshot(ctx, req)
		if err != nil {
			log.Printf("[Snapshot %d] ⚠️ CreateSnapshot RPC to master %d failed: %v. Initiating election.",
				s.nodeID, masterID, err)
			s.invalidatePeerConnection(masterID)
			s.masterID.Store(0)
			go s.startElection()
			return &filesync.CreateSnapshotResponse{
				Success: false,
				Message: fmt.Sprintf("Forward to master %d failed, election triggered: %v", masterID, err),
			}, nil
		}
		return resp, nil
	}

	// Generate unique snapshot ID.
	snapshotID := fmt.Sprintf("snap_%d_%d", time.Now().Unix(), s.nodeID)
	label := req.Label
	currentTerm := s.term.Load()

	log.Printf("[Snapshot %d] 📸 Creating snapshot '%s' (label: '%s')", s.nodeID, snapshotID, label)

	// Step 1: Perform master's local snapshot.
	walPos, err := s.performLocalSnapshot(snapshotID, label)
	if err != nil {
		return &filesync.CreateSnapshotResponse{
			Success: false,
			Message: fmt.Sprintf("Master snapshot failed: %v", err),
		}, nil
	}

	// Step 2: Broadcast SNAPSHOT-INIT to all followers.
	allPeers := s.getAllPeerIDs()
	ackCount := 1 // Master already done.
	totalNodes := len(allPeers) + 1
	participatingNodes := []int32{s.nodeID}

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peerID := range allPeers {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			client, err := s.getPeerClient(pid)
			if err != nil {
				log.Printf("[Snapshot %d] Cannot reach peer %d: %v", s.nodeID, pid, err)
				return
			}

			peerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := client.SnapshotInit(peerCtx, &filesync.SnapshotInitRequest{
				SnapshotId:  snapshotID,
				MasterId:    s.nodeID,
				InitiatedAt: startTime.UnixNano(),
				Label:       label,
				Term:        currentTerm,
			})

			if err != nil {
				log.Printf("[Snapshot %d] Peer %d snapshot failed: %v", s.nodeID, pid, err)
				return
			}

			if resp.Success {
				log.Printf("[MutEx %d] Acquiring local mu for snapshot ACK count", s.nodeID)
				mu.Lock()
				ackCount++
				participatingNodes = append(participatingNodes, pid)
				mu.Unlock()
				log.Printf("[MutEx %d] Released local mu for snapshot ACK count", s.nodeID)
				log.Printf("[Snapshot %d] Peer %d READY (WAL pos: %d, %dms)",
					s.nodeID, pid, resp.WalPosition, resp.SnapshotDurationMs)
			} else {
				log.Printf("[Snapshot %d] Peer %d FAILED: %s", s.nodeID, pid, resp.ErrorMessage)
			}
		}(peerID)
	}

	// Step 3: Wait for all ACKs.
	wg.Wait()
	totalElapsed := time.Since(startTime)

	if ackCount < totalNodes {
		log.Printf("[Snapshot %d] ⚠️ Snapshot '%s' incomplete (%d/%d nodes, %v)",
			s.nodeID, snapshotID, ackCount, totalNodes, totalElapsed)
	}

	// Step 4: Commit snapshot metadata.
	snapInfo := &metadata.SnapshotInfo{
		SnapshotID:         snapshotID,
		Label:              label,
		CreatedAt:          startTime.UnixNano(),
		InitiatedBy:        s.nodeID,
		WALPosition:        walPos,
		FileCount:          int32(s.fileIndex.Count()),
		TotalSizeBytes:     s.fileIndex.TotalSize(),
		Term:               currentTerm,
		ParticipatingNodes: participatingNodes,
	}

	log.Printf("[MutEx %d] Acquiring snapshotMu Lock for CreateSnapshot commit", s.nodeID)
	s.snapshotMu.Lock()
	s.snapshotMeta = append(s.snapshotMeta, snapInfo)
	s.snapshotMu.Unlock()
	log.Printf("[MutEx %d] Released snapshotMu Lock for CreateSnapshot commit", s.nodeID)

	// Persist snapshot metadata to disk.
	s.saveSnapshotMetadata()

	log.Printf("[Snapshot %d] ✅ Snapshot '%s' committed (%d/%d nodes, %v)",
		s.nodeID, snapshotID, ackCount, totalNodes, totalElapsed)

	return &filesync.CreateSnapshotResponse{
		Success:            true,
		SnapshotId:         snapshotID,
		Message:            fmt.Sprintf("Snapshot created with %d/%d nodes", ackCount, totalNodes),
		CoordinationTimeMs: totalElapsed.Milliseconds(),
		SnapshotInfo: &filesync.SnapshotInfo{
			SnapshotId:         snapshotID,
			Label:              label,
			CreatedAt:          startTime.UnixNano(),
			InitiatedBy:        s.nodeID,
			WalPosition:        walPos,
			FileCount:          int32(s.fileIndex.Count()),
			TotalSizeBytes:     s.fileIndex.TotalSize(),
			Term:               currentTerm,
			ParticipatingNodes: participatingNodes,
		},
	}, nil
}

// performLocalSnapshot atomically copies the file index to a snapshot file.
func (s *Server) performLocalSnapshot(snapshotID, label string) (int64, error) {
	// Get current WAL position.
	walPos, err := s.wal.GetLatestEntryID()
	if err != nil {
		return 0, fmt.Errorf("failed to get WAL position: %w", err)
	}

	// Atomically copy the file index.
	snapshot := s.fileIndex.Snapshot()
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	snapshotFile := filepath.Join(s.dirs.Snapshots, fmt.Sprintf("snapshot_%s.json", snapshotID))
	if err := os.WriteFile(snapshotFile, data, 0644); err != nil {
		return 0, fmt.Errorf("failed to write snapshot file: %w", err)
	}

	log.Printf("[Snapshot %d] Local snapshot '%s' saved to %s", s.nodeID, snapshotID, snapshotFile)
	return walPos, nil
}

// ----------- SnapshotInit Handler (Follower Side) -----------

// SnapshotInit handles incoming snapshot requests from the master.
func (s *Server) SnapshotInit(ctx context.Context, req *filesync.SnapshotInitRequest) (*filesync.SnapshotInitResponse, error) {
	startTime := time.Now()
	log.Printf("[Snapshot %d] Received SNAPSHOT-INIT '%s' from master %d",
		s.nodeID, req.SnapshotId, req.MasterId)

	// Perform local snapshot.
	walPos, err := s.performLocalSnapshot(req.SnapshotId, req.Label)
	if err != nil {
		return &filesync.SnapshotInitResponse{
			NodeId:       s.nodeID,
			Success:      false,
			ErrorMessage: fmt.Sprintf("Snapshot failed: %v", err),
		}, nil
	}

	// Store snapshot metadata locally.
	snapInfo := &metadata.SnapshotInfo{
		SnapshotID:  req.SnapshotId,
		Label:       req.Label,
		CreatedAt:   time.Now().UnixNano(),
		InitiatedBy: req.MasterId,
		WALPosition: walPos,
		FileCount:   int32(s.fileIndex.Count()),
		Term:        req.Term,
	}

	log.Printf("[MutEx %d] Acquiring snapshotMu Lock for SnapshotInit commit", s.nodeID)
	s.snapshotMu.Lock()
	s.snapshotMeta = append(s.snapshotMeta, snapInfo)
	s.snapshotMu.Unlock()
	log.Printf("[MutEx %d] Released snapshotMu Lock for SnapshotInit commit", s.nodeID)

	s.saveSnapshotMetadata()

	elapsed := time.Since(startTime).Milliseconds()
	log.Printf("[Snapshot %d] ✅ Local snapshot '%s' complete (%dms)",
		s.nodeID, req.SnapshotId, elapsed)

	return &filesync.SnapshotInitResponse{
		NodeId:             s.nodeID,
		Success:            true,
		WalPosition:        walPos,
		SnapshotDurationMs: elapsed,
	}, nil
}

// ----------- ListSnapshots Handler -----------

// ListSnapshots returns metadata for all available snapshots.
func (s *Server) ListSnapshots(ctx context.Context, req *filesync.ListSnapshotsRequest) (*filesync.ListSnapshotsResponse, error) {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()

	protoSnapshots := make([]*filesync.SnapshotInfo, 0, len(s.snapshotMeta))
	for _, snap := range s.snapshotMeta {
		protoSnapshots = append(protoSnapshots, &filesync.SnapshotInfo{
			SnapshotId:         snap.SnapshotID,
			Label:              snap.Label,
			CreatedAt:          snap.CreatedAt,
			InitiatedBy:        snap.InitiatedBy,
			WalPosition:        snap.WALPosition,
			FileCount:          snap.FileCount,
			TotalSizeBytes:     snap.TotalSizeBytes,
			Term:               snap.Term,
			ParticipatingNodes: snap.ParticipatingNodes,
		})
	}

	log.Printf("[Snapshot %d] ListSnapshots: %d snapshots", s.nodeID, len(protoSnapshots))

	return &filesync.ListSnapshotsResponse{
		Snapshots:  protoSnapshots,
		TotalCount: int32(len(protoSnapshots)),
	}, nil
}

// ----------- ReadSnapshot Handler -----------

// ReadSnapshot retrieves the immutable file index from a specific snapshot.
func (s *Server) ReadSnapshot(ctx context.Context, req *filesync.ReadSnapshotRequest) (*filesync.ReadSnapshotResponse, error) {
	log.Printf("[Snapshot %d] ReadSnapshot '%s'", s.nodeID, req.SnapshotId)

	// Find the snapshot metadata.
	s.snapshotMu.RLock()
	var snapInfo *metadata.SnapshotInfo
	for _, snap := range s.snapshotMeta {
		if snap.SnapshotID == req.SnapshotId {
			snapCopy := *snap
			snapInfo = &snapCopy
			break
		}
	}
	s.snapshotMu.RUnlock()

	if snapInfo == nil {
		return &filesync.ReadSnapshotResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Snapshot '%s' not found", req.SnapshotId),
		}, nil
	}

	// Read the snapshot file from disk.
	snapshotFile := filepath.Join(s.dirs.Snapshots,
		fmt.Sprintf("snapshot_%s.json", req.SnapshotId))

	data, err := os.ReadFile(snapshotFile)
	if err != nil {
		return &filesync.ReadSnapshotResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Failed to read snapshot file: %v", err),
		}, nil
	}

	var fileIndex map[string]*metadata.FileMetadata
	if err := json.Unmarshal(data, &fileIndex); err != nil {
		return &filesync.ReadSnapshotResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Failed to parse snapshot: %v", err),
		}, nil
	}

	// Convert to protobuf.
	protoFiles := make([]*filesync.FileMetadataProto, 0, len(fileIndex))
	for _, f := range fileIndex {
		protoFiles = append(protoFiles, &filesync.FileMetadataProto{
			Filename:  f.Filename,
			Version:   f.Version,
			Size:      f.Size,
			Checksum:  f.Checksum,
			Replicas:  f.Replicas,
			CreatedAt: f.CreatedAt,
			UpdatedAt: f.UpdatedAt,
		})
	}

	return &filesync.ReadSnapshotResponse{
		Success: true,
		SnapshotInfo: &filesync.SnapshotInfo{
			SnapshotId:         snapInfo.SnapshotID,
			Label:              snapInfo.Label,
			CreatedAt:          snapInfo.CreatedAt,
			InitiatedBy:        snapInfo.InitiatedBy,
			WalPosition:        snapInfo.WALPosition,
			FileCount:          snapInfo.FileCount,
			TotalSizeBytes:     snapInfo.TotalSizeBytes,
			Term:               snapInfo.Term,
			ParticipatingNodes: snapInfo.ParticipatingNodes,
		},
		FileIndex: protoFiles,
	}, nil
}

// ----------- Helpers -----------

// saveSnapshotMetadata persists the snapshot metadata list to disk.
func (s *Server) saveSnapshotMetadata() {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()

	data, err := json.MarshalIndent(s.snapshotMeta, "", "  ")
	if err != nil {
		log.Printf("[Snapshot %d] Failed to marshal snapshot metadata: %v", s.nodeID, err)
		return
	}

	metaFile := filepath.Join(s.dirs.Metadata, "snapshots_meta.json")
	if err := os.WriteFile(metaFile, data, 0644); err != nil {
		log.Printf("[Snapshot %d] Failed to write snapshot metadata: %v", s.nodeID, err)
	}
}

// loadSnapshotMetadata loads previously saved snapshot metadata from disk.
func (s *Server) loadSnapshotMetadata() {
	metaFile := filepath.Join(s.dirs.Metadata, "snapshots_meta.json")
	data, err := os.ReadFile(metaFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[Snapshot %d] Warning: failed to load snapshot metadata: %v", s.nodeID, err)
		}
		return
	}

	log.Printf("[MutEx %d] Acquiring snapshotMu Lock for loadSnapshotMetadata", s.nodeID)
	s.snapshotMu.Lock()
	defer func() {
		s.snapshotMu.Unlock()
		log.Printf("[MutEx %d] Released snapshotMu Lock for loadSnapshotMetadata", s.nodeID)
	}()

	if err := json.Unmarshal(data, &s.snapshotMeta); err != nil {
		log.Printf("[Snapshot %d] Warning: failed to parse snapshot metadata: %v", s.nodeID, err)
	} else {
		log.Printf("[Snapshot %d] Loaded %d snapshot records", s.nodeID, len(s.snapshotMeta))
	}
}

// getLatestCheckpointID scans the metadata directory for checkpoint files
// and returns the highest checkpoint ID found.
func (s *Server) getLatestCheckpointID() (int64, error) {
	entries, err := os.ReadDir(s.dirs.Metadata)
	if err != nil {
		return 0, err
	}

	var maxID int64
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "checkpoint_") &&
			strings.HasSuffix(entry.Name(), ".json") {
			var id int64
			fmt.Sscanf(entry.Name(), "checkpoint_%d.json", &id)
			if id > maxID {
				maxID = id
			}
		}
	}

	return maxID, nil
}

// getLatestSnapshotID returns the most recent snapshot ID by creation time.
func (s *Server) getLatestSnapshotID() string {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()

	if len(s.snapshotMeta) == 0 {
		return ""
	}

	// Sort by creation time (descending) and return the latest.
	sorted := make([]*metadata.SnapshotInfo, len(s.snapshotMeta))
	copy(sorted, s.snapshotMeta)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt > sorted[j].CreatedAt
	})

	return sorted[0].SnapshotID
}
