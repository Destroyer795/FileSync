// =============================================================================
// Filesystem Snapshots — Chandy-Lamport Distributed Snapshot Algorithm
// =============================================================================
//
// Implements the Chandy-Lamport algorithm for consistent distributed snapshots:
//
// 1. INITIATE: Any node (typically the master) can initiate a snapshot.
//    The initiator:
//    a) Records its own local state (file index + WAL position).
//    b) Sends a MARKER message on all outgoing channels (to all peers).
//    c) Begins recording incoming messages on all channels.
//
// 2. MARKER RECEIPT: When a node receives a MARKER on channel C:
//    a) If this is the FIRST marker for this snapshot ID:
//       - The node records its own local state.
//       - Sends MARKER messages on all its outgoing channels.
//       - Starts recording messages on all incoming channels EXCEPT C.
//       - Records channel C's state as empty (no messages in transit).
//    b) If the node has ALREADY recorded its state for this snapshot:
//       - Stops recording on channel C.
//       - Saves all messages recorded on C as the channel state.
//
// 3. COMPLETION: The snapshot is complete at a node when it has received
//    MARKER messages on all incoming channels. The global snapshot consists
//    of all local states plus all channel states.
//
// In FileSync's context:
//   - "Local state" = file index snapshot + WAL position
//   - "Channels" = gRPC connections between peer nodes
//   - "Messages" = file operation events (uploads/deletes) in transit
//
// Tagged with [CL-Snapshot] in log messages for identification.
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

// ----------- Chandy-Lamport Snapshot State -----------

// clSnapshotState tracks the in-progress state of a Chandy-Lamport snapshot
// for a particular snapshot ID on this node.
type clSnapshotState struct {
	snapshotID string
	recorded   bool   // Whether this node has recorded its local state.
	label      string // User-provided label for the snapshot.

	// Channel states: maps peer node ID -> list of operations observed
	// on that channel between our state recording and receiving their MARKER.
	channelState map[int32][]string // peerID -> recorded operation descriptions
	// Which peers have we received MARKERs from?
	markersReceived map[int32]bool
	// Total number of peers we expect MARKERs from.
	totalPeers int
	// Who initiated this snapshot.
	initiatedBy int32
	// WAL position at time of state recording.
	walPosition int64
	// When the snapshot started.
	startTime time.Time

	mu sync.Mutex
}

// activeSnapshots tracks all in-progress Chandy-Lamport snapshots.
// Key is the snapshot ID.
var activeSnapshots = struct {
	mu sync.Mutex
	m  map[string]*clSnapshotState
}{m: make(map[string]*clSnapshotState)}

// getOrCreateCLState returns the snapshot state for the given ID,
// creating it if it doesn't exist.
func getOrCreateCLState(snapshotID string, totalPeers int) *clSnapshotState {
	activeSnapshots.mu.Lock()
	defer activeSnapshots.mu.Unlock()

	if state, ok := activeSnapshots.m[snapshotID]; ok {
		return state
	}

	state := &clSnapshotState{
		snapshotID:      snapshotID,
		channelState:    make(map[int32][]string),
		markersReceived: make(map[int32]bool),
		totalPeers:      totalPeers,
		startTime:       time.Now(),
	}
	activeSnapshots.m[snapshotID] = state
	return state
}

// removeCLState removes a completed snapshot state.
func removeCLState(snapshotID string) {
	activeSnapshots.mu.Lock()
	defer activeSnapshots.mu.Unlock()
	delete(activeSnapshots.m, snapshotID)
}

// RecordChannelMessage records an in-flight operation on a channel from
// the given peer, if there is an active snapshot where we have recorded
// our state but have not yet received a MARKER from that peer.
func (s *Server) RecordChannelMessage(fromPeerID int32, opDescription string) {
	activeSnapshots.mu.Lock()
	snapshots := make([]*clSnapshotState, 0)
	for _, state := range activeSnapshots.m {
		snapshots = append(snapshots, state)
	}
	activeSnapshots.mu.Unlock()

	for _, state := range snapshots {
		state.mu.Lock()
		if state.recorded && !state.markersReceived[fromPeerID] {
			// We have recorded our state but haven't received MARKER from this peer.
			// Record this message as part of the channel state.
			state.channelState[fromPeerID] = append(state.channelState[fromPeerID], opDescription)
			log.Printf("[CL-Snapshot %d] Recorded channel message from peer %d: %s (snapshot '%s')",
				s.nodeID, fromPeerID, opDescription, state.snapshotID)
		}
		state.mu.Unlock()
	}
}

// ----------- CreateSnapshot Handler (Initiator) -----------

// CreateSnapshot initiates a Chandy-Lamport distributed snapshot.
// The initiator records its own state and sends MARKER to all peers.
func (s *Server) CreateSnapshot(ctx context.Context, req *filesync.CreateSnapshotRequest) (*filesync.CreateSnapshotResponse, error) {
	startTime := time.Now()

	// Generate unique snapshot ID.
	snapshotID := fmt.Sprintf("snap_%d_%d", time.Now().Unix(), s.nodeID)
	label := req.Label
	currentTerm := s.term.Load()
	allPeers := s.getAllPeerIDs()

	log.Printf("[CL-Snapshot %d] Initiating Chandy-Lamport snapshot '%s' (label: '%s')",
		s.nodeID, snapshotID, label)

	// Step 1: Record own local state (Chandy-Lamport rule: initiator records first).
	state := getOrCreateCLState(snapshotID, len(allPeers))
	state.mu.Lock()
	state.recorded = true
	state.label = label
	state.initiatedBy = s.nodeID
	state.mu.Unlock()

	walPos, err := s.performLocalSnapshot(snapshotID, label)
	if err != nil {
		removeCLState(snapshotID)
		return &filesync.CreateSnapshotResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to record local state: %v", err),
		}, nil
	}

	state.mu.Lock()
	state.walPosition = walPos
	state.mu.Unlock()

	log.Printf("[CL-Snapshot %d] Local state recorded (WAL pos: %d). Sending MARKER to %d peers.",
		s.nodeID, walPos, len(allPeers))

	// Step 2: Send MARKER to all outgoing channels (all peers).
	ackCount := 1 // We count ourselves.
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
				log.Printf("[CL-Snapshot %d] Cannot reach peer %d to send MARKER: %v",
					s.nodeID, pid, err)
				return
			}

			peerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			log.Printf("[CL-Snapshot %d] Sending MARKER to peer %d (snapshot '%s')",
				s.nodeID, pid, snapshotID)

			resp, err := client.SnapshotInit(peerCtx, &filesync.SnapshotInitRequest{
				SnapshotId:  snapshotID,
				MasterId:    s.nodeID,
				InitiatedAt: startTime.UnixNano(),
				Label:       label,
				Term:        currentTerm,
			})

			if err != nil {
				log.Printf("[CL-Snapshot %d] MARKER to peer %d failed: %v",
					s.nodeID, pid, err)
				return
			}

			if resp.Success {
				mu.Lock()
				ackCount++
				participatingNodes = append(participatingNodes, pid)
				mu.Unlock()

				log.Printf("[CL-Snapshot %d] Peer %d recorded state (WAL pos: %d, %dms)",
					s.nodeID, pid, resp.WalPosition, resp.SnapshotDurationMs)
			} else {
				log.Printf("[CL-Snapshot %d] Peer %d failed to process MARKER: %s",
					s.nodeID, pid, resp.ErrorMessage)
			}
		}(peerID)
	}

	// Step 3: Wait for all MARKERs to be processed.
	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Mark all peers as having sent us markers (as initiator, we consider all
	// channels complete once peers have responded to our MARKER).
	state.mu.Lock()
	for _, pid := range allPeers {
		state.markersReceived[pid] = true
	}

	// Collect channel states for logging.
	totalChannelMsgs := 0
	for pid, msgs := range state.channelState {
		if len(msgs) > 0 {
			log.Printf("[CL-Snapshot %d] Channel state from peer %d: %d messages recorded",
				s.nodeID, pid, len(msgs))
			totalChannelMsgs += len(msgs)
		}
	}
	state.mu.Unlock()

	if ackCount < totalNodes {
		log.Printf("[CL-Snapshot %d] Snapshot '%s' incomplete (%d/%d nodes, %v)",
			s.nodeID, snapshotID, ackCount, totalNodes, totalElapsed)
	}

	log.Printf("[CL-Snapshot %d] Snapshot '%s' complete: %d/%d nodes, %d channel messages captured, %v",
		s.nodeID, snapshotID, ackCount, totalNodes, totalChannelMsgs, totalElapsed)

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

	s.saveSnapshotMetadata()

	// Clean up the in-progress state.
	removeCLState(snapshotID)

	return &filesync.CreateSnapshotResponse{
		Success:            true,
		SnapshotId:         snapshotID,
		Message:            fmt.Sprintf("Chandy-Lamport snapshot with %d/%d nodes", ackCount, totalNodes),
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

	log.Printf("[CL-Snapshot %d] Local state saved to %s", s.nodeID, snapshotFile)
	return walPos, nil
}

// ----------- SnapshotInit Handler (MARKER Receiver) -----------

// SnapshotInit handles incoming MARKER messages from peers.
// Per Chandy-Lamport:
//   - First MARKER for this snapshot: record own state, forward MARKERs, mark
//     channel from sender as empty.
//   - Subsequent MARKER: stop recording on that channel, save channel state.
func (s *Server) SnapshotInit(ctx context.Context, req *filesync.SnapshotInitRequest) (*filesync.SnapshotInitResponse, error) {
	startTime := time.Now()
	senderID := req.MasterId

	log.Printf("[CL-Snapshot %d] Received MARKER from node %d for snapshot '%s'",
		s.nodeID, senderID, req.SnapshotId)

	allPeers := s.getAllPeerIDs()
	state := getOrCreateCLState(req.SnapshotId, len(allPeers))

	state.mu.Lock()
	firstMarker := !state.recorded
	state.mu.Unlock()

	if firstMarker {
		// First MARKER: record own local state.
		log.Printf("[CL-Snapshot %d] First MARKER received (from node %d). Recording local state.",
			s.nodeID, senderID)

		state.mu.Lock()
		state.recorded = true
		state.label = req.Label
		state.initiatedBy = req.MasterId
		state.mu.Unlock()

		walPos, err := s.performLocalSnapshot(req.SnapshotId, req.Label)
		if err != nil {
			log.Printf("[CL-Snapshot %d] Failed to record local state: %v", s.nodeID, err)
			return &filesync.SnapshotInitResponse{
				NodeId:       s.nodeID,
				Success:      false,
				ErrorMessage: fmt.Sprintf("Failed to record state: %v", err),
			}, nil
		}

		state.mu.Lock()
		state.walPosition = walPos
		// Mark the sender's channel as empty (no messages in transit on this
		// channel since we recorded state immediately upon receiving the MARKER).
		state.markersReceived[senderID] = true
		state.channelState[senderID] = []string{} // Empty channel state.
		state.mu.Unlock()

		log.Printf("[CL-Snapshot %d] State recorded (WAL pos: %d). Channel from node %d marked empty.",
			s.nodeID, walPos, senderID)

		// Forward MARKER to all other peers (not back to sender).
		for _, pid := range allPeers {
			if pid == senderID {
				continue // Don't send back to the sender.
			}
			go func(peerID int32) {
				client, err := s.getPeerClient(peerID)
				if err != nil {
					log.Printf("[CL-Snapshot %d] Cannot forward MARKER to peer %d: %v",
						s.nodeID, peerID, err)
					return
				}

				fwdCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				log.Printf("[CL-Snapshot %d] Forwarding MARKER to peer %d (snapshot '%s')",
					s.nodeID, peerID, req.SnapshotId)

				_, err = client.SnapshotInit(fwdCtx, &filesync.SnapshotInitRequest{
					SnapshotId:  req.SnapshotId,
					MasterId:    s.nodeID, // We are the sender now.
					InitiatedAt: req.InitiatedAt,
					Label:       req.Label,
					Term:        req.Term,
				})

				if err != nil {
					log.Printf("[CL-Snapshot %d] MARKER forwarding to peer %d failed: %v",
						s.nodeID, peerID, err)
				} else {
					log.Printf("[CL-Snapshot %d] MARKER forwarded to peer %d",
						s.nodeID, peerID)
				}
			}(pid)
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
		log.Printf("[CL-Snapshot %d] Snapshot '%s' local processing complete (%dms)",
			s.nodeID, req.SnapshotId, elapsed)

		return &filesync.SnapshotInitResponse{
			NodeId:             s.nodeID,
			Success:            true,
			WalPosition:        walPos,
			SnapshotDurationMs: elapsed,
		}, nil
	}

	// Subsequent MARKER: we've already recorded our state.
	// Stop recording on this channel and save the channel state.
	state.mu.Lock()
	state.markersReceived[senderID] = true
	channelMsgs := state.channelState[senderID]
	allReceived := len(state.markersReceived) >= state.totalPeers
	state.mu.Unlock()

	if len(channelMsgs) > 0 {
		log.Printf("[CL-Snapshot %d] Subsequent MARKER from node %d. Channel state: %d messages recorded.",
			s.nodeID, senderID, len(channelMsgs))
	} else {
		log.Printf("[CL-Snapshot %d] Subsequent MARKER from node %d. Channel state: empty (no in-flight messages).",
			s.nodeID, senderID)
	}

	if allReceived {
		log.Printf("[CL-Snapshot %d] All MARKERs received for snapshot '%s'. Snapshot complete at this node.",
			s.nodeID, req.SnapshotId)
		removeCLState(req.SnapshotId)
	}

	elapsed := time.Since(startTime).Milliseconds()
	return &filesync.SnapshotInitResponse{
		NodeId:             s.nodeID,
		Success:            true,
		WalPosition:        state.walPosition,
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

	log.Printf("[CL-Snapshot %d] ListSnapshots: %d snapshots", s.nodeID, len(protoSnapshots))

	return &filesync.ListSnapshotsResponse{
		Snapshots:  protoSnapshots,
		TotalCount: int32(len(protoSnapshots)),
	}, nil
}

// ----------- ReadSnapshot Handler -----------

// ReadSnapshot retrieves the immutable file index from a specific snapshot.
func (s *Server) ReadSnapshot(ctx context.Context, req *filesync.ReadSnapshotRequest) (*filesync.ReadSnapshotResponse, error) {
	log.Printf("[CL-Snapshot %d] ReadSnapshot '%s'", s.nodeID, req.SnapshotId)

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
		log.Printf("[CL-Snapshot %d] Failed to marshal snapshot metadata: %v", s.nodeID, err)
		return
	}

	metaFile := filepath.Join(s.dirs.Metadata, "snapshots_meta.json")
	if err := os.WriteFile(metaFile, data, 0644); err != nil {
		log.Printf("[CL-Snapshot %d] Failed to write snapshot metadata: %v", s.nodeID, err)
	}
}

// loadSnapshotMetadata loads previously saved snapshot metadata from disk.
func (s *Server) loadSnapshotMetadata() {
	metaFile := filepath.Join(s.dirs.Metadata, "snapshots_meta.json")
	data, err := os.ReadFile(metaFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[CL-Snapshot %d] Warning: failed to load snapshot metadata: %v", s.nodeID, err)
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
		log.Printf("[CL-Snapshot %d] Warning: failed to parse snapshot metadata: %v", s.nodeID, err)
	} else {
		log.Printf("[CL-Snapshot %d] Loaded %d snapshot records", s.nodeID, len(s.snapshotMeta))
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
