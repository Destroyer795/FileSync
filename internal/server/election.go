// =============================================================================
// Bully Leader Election & Heartbeat Mechanism
// =============================================================================
//
// Implements the Bully Algorithm for leader election in the FileSync cluster:
//
// 1. HEARTBEAT: The master broadcasts heartbeats every 200ms to all followers.
//    Each follower tracks the last received heartbeat timestamp.
//
// 2. FAILURE DETECTION: If a follower receives no heartbeat for 300ms, it
//    assumes the master has failed and initiates an election.
//
// 3. BULLY ELECTION (Random Priority):
//    Each node is assigned a random election priority at startup.
//    a) The detecting node sends ELECTION messages to ALL other nodes.
//    b) Any node with a HIGHER priority that is alive responds with OK
//       and takes over the election.
//    c) If NO higher-priority node responds within the election timeout,
//       the initiator declares itself the new master and broadcasts
//       COORDINATOR (VictoryRequest) to all nodes.
//    d) On receiving a COORDINATOR message, all nodes update their masterID.
//
// Convergence target: 85ms (actual depends on network RTT).
// =============================================================================

package server

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/filesync/gen/filesync"
)

// ----------- gRPC Handlers -----------

// Heartbeat handles incoming heartbeat requests from the master.
// Updates the last heartbeat timestamp so the failure detector doesn't
// trigger a spurious election.
func (s *Server) Heartbeat(ctx context.Context, req *filesync.HeartbeatRequest) (*filesync.HeartbeatResponse, error) {
	receiveTime := time.Now()

	// Only accept heartbeats from the recognized master or during initial setup.
	currentMaster := s.getMasterID()
	if currentMaster != 0 && req.MasterId != currentMaster {
		// Stale heartbeat from an old master — check term.
		if req.Term < s.term.Load() {
			log.Printf("[Election %d] Rejected stale heartbeat from node %d (term %d < %d)",
				s.nodeID, req.MasterId, req.Term, s.term.Load())
			return &filesync.HeartbeatResponse{
				NodeId:       s.nodeID,
				AckTimestamp: receiveTime.UnixNano(),
				Acknowledged: false,
			}, nil
		}
	}

	// Update the last heartbeat timestamp — resets the failure detection timer.
	s.lastHeartbeat.Store(receiveTime.UnixNano())

	// Accept this sender as master if its term is >= ours.
	if req.Term >= s.term.Load() {
		s.masterID.Store(req.MasterId)
		s.term.Store(req.Term)
		if s.nodeID != req.MasterId {
			s.isMaster.Store(false)
		}
	}

	return &filesync.HeartbeatResponse{
		NodeId:       s.nodeID,
		AckTimestamp: receiveTime.UnixNano(),
		Acknowledged: true,
	}, nil
}

// Election handles incoming ELECTION messages from other nodes.
// Per the Bully Algorithm with random priorities:
//   - If our priority is HIGHER than the candidate's, respond alive=true
//     and take over the election ourselves.
//   - If our priority is LOWER, respond alive=false (we yield).
func (s *Server) Election(ctx context.Context, req *filesync.ElectionRequest) (*filesync.ElectionResponse, error) {
	myPriority := s.getElectionPriority()
	candidatePriority := req.Term // We reuse the 'term' field to carry priority.

	log.Printf("[Election %d] Received ELECTION from node %d (their priority: %d, my priority: %d)",
		s.nodeID, req.CandidateId, candidatePriority, myPriority)

	// Store this peer's priority for future reference.
	s.setPeerPriority(req.CandidateId, candidatePriority)

	if myPriority > candidatePriority {
		// We have higher priority — respond "alive" and take over the election.
		log.Printf("[Election %d] I have higher priority (%d > %d), taking over election",
			s.nodeID, myPriority, candidatePriority)
		go s.startElection()

		return &filesync.ElectionResponse{
			ResponderId: s.nodeID,
			Alive:       true,
			Term:        myPriority, // Send our priority back.
		}, nil
	}

	// We have lower priority — yield.
	log.Printf("[Election %d] I have lower priority (%d <= %d), yielding",
		s.nodeID, myPriority, candidatePriority)
	return &filesync.ElectionResponse{
		ResponderId: s.nodeID,
		Alive:       false,
		Term:        myPriority,
	}, nil
}

// ElectionVictory handles incoming COORDINATOR messages from the election winner.
// All nodes update their masterID to the new master.
func (s *Server) ElectionVictory(ctx context.Context, req *filesync.VictoryRequest) (*filesync.VictoryResponse, error) {
	log.Printf("[Election %d] Received COORDINATOR from node %d (term %d)",
		s.nodeID, req.NewMasterId, req.Term)

	// Accept the new master (update regardless of term for simplicity).
	s.masterID.Store(req.NewMasterId)
	s.term.Store(req.Term)
	s.lastHeartbeat.Store(time.Now().UnixNano())

	if req.NewMasterId == s.nodeID {
		s.isMaster.Store(true)
		log.Printf("[Election %d] I am the new master (term %d)", s.nodeID, req.Term)
	} else {
		s.isMaster.Store(false)
		// Look up the new master's IP for the log.
		masterIP := ""
		if addr, ok := s.peerAddrs[req.NewMasterId]; ok {
			masterIP = addr
		}
		log.Printf("[Election %d] ✅ Accepted node %d (IP: %s) as new master (term %d)",
			s.nodeID, req.NewMasterId, masterIP, req.Term)

		// Record the leadership change in our local master log.
		s.writeRemoteMasterLogEntry(req.NewMasterId, req.Term)
	}

	return &filesync.VictoryResponse{
		NodeId:   s.nodeID,
		Accepted: true,
	}, nil
}

// ----------- Election Logic -----------

// startElection initiates a Bully election from this node.
// Sends ELECTION messages to ALL peer nodes. Any peer with a higher
// random priority will respond alive=true and take over. If no higher-
// priority peer responds, this node declares itself master.
func (s *Server) startElection() {
	// Prevent concurrent elections from this node.
	if !s.electionInProgress.CompareAndSwap(false, true) {
		log.Printf("[Election %d] Election already in progress, skipping", s.nodeID)
		return
	}
	defer s.electionInProgress.Store(false)

	electionStart := time.Now()
	myPriority := s.getElectionPriority()
	log.Printf("[Election %d] 🗳️ Initiating Bully election (my priority: %d)",
		s.nodeID, myPriority)

	// Send ELECTION messages to ALL peer nodes (not just higher-ID ones,
	// since we use random priorities and don't know who's higher).
	allPeers := s.getAllPeerIDs()

	if len(allPeers) == 0 {
		log.Printf("[Election %d] No peers configured, declaring victory", s.nodeID)
		s.declareVictory(myPriority, electionStart)
		return
	}

	// Send ELECTION messages to all peers concurrently.
	type electionResult struct {
		peerID       int32
		alive        bool // true if this peer has higher priority and is taking over
		peerPriority int64
		reachable    bool // true if peer responded at all
	}

	resultCh := make(chan electionResult, len(allPeers))
	var wg sync.WaitGroup

	for _, peerID := range allPeers {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			client, err := s.getPeerClient(pid)
			if err != nil {
				log.Printf("[Election %d] Cannot reach peer %d: %v", s.nodeID, pid, err)
				s.invalidatePeerConnection(pid) // Clean up stale connection.
				resultCh <- electionResult{peerID: pid, alive: false, reachable: false}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ElectionTimeout())
			defer cancel()

			resp, err := client.Election(ctx, &filesync.ElectionRequest{
				CandidateId: s.nodeID,
				Term:        myPriority, // Send our priority in the term field.
				InitiatedAt: electionStart.UnixNano(),
			})

			if err != nil {
				log.Printf("[Election %d] Peer %d did not respond: %v", s.nodeID, pid, err)
				s.invalidatePeerConnection(pid) // Clean up on failure.
				resultCh <- electionResult{peerID: pid, alive: false, reachable: false}
				return
			}

			// Store this peer's priority.
			s.setPeerPriority(pid, resp.Term)

			resultCh <- electionResult{
				peerID:       pid,
				alive:        resp.Alive, // true = this peer has higher priority
				peerPriority: resp.Term,
				reachable:    true,
			}
		}(peerID)
	}

	// Close the channel once all goroutines complete.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Wait for responses with a timeout.
	timeout := time.After(s.cfg.ElectionTimeout())
	higherPriorityExists := false

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				// Channel closed — all responses received.
				goto done
			}
			if result.alive {
				log.Printf("[Election %d] Peer %d has higher priority (%d > %d), backing off",
					s.nodeID, result.peerID, result.peerPriority, myPriority)
				higherPriorityExists = true
				goto done
			}
		case <-timeout:
			log.Printf("[Election %d] Election timeout reached (%v)",
				s.nodeID, s.cfg.ElectionTimeout())
			goto done
		}
	}

done:
	if !higherPriorityExists {
		// No higher-priority peer responded — we win!
		s.declareVictory(myPriority, electionStart)
	} else {
		log.Printf("[Election %d] Backed off — waiting for higher-priority node to become master",
			s.nodeID)
	}
}

// declareVictory broadcasts COORDINATOR messages to all peers, announcing
// this node as the new master.
func (s *Server) declareVictory(priority int64, electionStart time.Time) {
	convergenceTime := time.Since(electionStart)
	newTerm := s.term.Add(1)

	log.Printf("[Election %d] 🏆 Declaring victory! Priority=%d, Term=%d, convergence=%v",
		s.nodeID, priority, newTerm, convergenceTime)

	// Update local state.
	s.isMaster.Store(true)
	s.masterID.Store(s.nodeID)
	s.lastHeartbeat.Store(time.Now().UnixNano())

	// Write the master election record to master_log.jsonl.
	s.recordMasterElection(newTerm)

	// Broadcast COORDINATOR to all peers.
	allPeers := s.getAllPeerIDs()
	var wg sync.WaitGroup

	for _, peerID := range allPeers {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			client, err := s.getPeerClient(pid)
			if err != nil {
				log.Printf("[Election %d] Cannot notify peer %d of victory: %v",
					s.nodeID, pid, err)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			resp, err := client.ElectionVictory(ctx, &filesync.VictoryRequest{
				NewMasterId: s.nodeID,
				Term:        newTerm,
				DeclaredAt:  time.Now().UnixNano(),
			})

			if err != nil {
				log.Printf("[Election %d] Failed to notify peer %d: %v", s.nodeID, pid, err)
				s.invalidatePeerConnection(pid)
			} else if resp.Accepted {
				log.Printf("[Election %d] Peer %d accepted our leadership", s.nodeID, pid)
			}
		}(peerID)
	}

	wg.Wait()
	log.Printf("[Election %d] Victory broadcast complete. Starting heartbeat loop.",
		s.nodeID)

	// Start broadcasting heartbeats as the new master.
	go s.heartbeatBroadcastLoop()
}

// ----------- Heartbeat Loops -----------

// heartbeatBroadcastLoop runs on the master and sends heartbeats to all
// followers every 200ms. Stops when this node is no longer the master.
func (s *Server) heartbeatBroadcastLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval())
	defer ticker.Stop()

	log.Printf("[Heartbeat %d] Starting master heartbeat broadcast (every %v)",
		s.nodeID, s.cfg.HeartbeatInterval())

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("[Heartbeat %d] Heartbeat loop stopped (context cancelled)", s.nodeID)
			return
		case <-ticker.C:
			if !s.isMasterNode() {
				log.Printf("[Heartbeat %d] No longer master, stopping heartbeat loop", s.nodeID)
				return
			}
			s.broadcastHeartbeat()
		}
	}
}

// broadcastHeartbeat sends a heartbeat to all peer nodes.
// If a peer fails to respond, invalidates its connection for reconnection.
func (s *Server) broadcastHeartbeat() {
	sendTime := time.Now()
	currentTerm := s.term.Load()

	allPeers := s.getAllPeerIDs()
	for _, peerID := range allPeers {
		go func(pid int32) {
			client, err := s.getPeerClient(pid)
			if err != nil {
				// Peer unreachable — invalidate and try fresh next time.
				// This is expected when a node is down (fault tolerance).
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2000*time.Millisecond)
			defer cancel()

			resp, err := client.Heartbeat(ctx, &filesync.HeartbeatRequest{
				MasterId:  s.nodeID,
				Timestamp: sendTime.UnixNano(),
				Term:      currentTerm,
			})

			if err != nil {
				// Peer failed — invalidate connection so next attempt reconnects.
				log.Printf("[Heartbeat %d] Peer %d unreachable: %v", s.nodeID, pid, err)
				s.invalidatePeerConnection(pid)
				return
			}

			// Calculate RTT from response.
			rtt := time.Duration(resp.AckTimestamp-sendTime.UnixNano()) * time.Nanosecond
			if rtt < 0 {
				rtt = time.Since(sendTime) // Fallback if clocks are skewed.
			}
			_ = rtt // Available for future metrics dashboarding.
		}(peerID)
	}
}

// heartbeatMonitorLoop runs on follower nodes and checks for master liveness.
// If no heartbeat is received within 300ms, it triggers a Bully election.
func (s *Server) heartbeatMonitorLoop() {
	// Check every 100ms for heartbeat timeout.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	log.Printf("[Heartbeat %d] Starting heartbeat monitor (timeout: %v)",
		s.nodeID, s.cfg.HeartbeatTimeout())

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Masters don't need to monitor themselves.
			if s.isMasterNode() {
				continue
			}

			// Don't monitor until we have a master.
			if s.getMasterID() == 0 {
				continue
			}

			lastHB := time.Unix(0, s.lastHeartbeat.Load())
			elapsed := time.Since(lastHB)

			if elapsed > s.cfg.HeartbeatTimeout() {
				oldMaster := s.getMasterID()
				oldMasterAddr := s.getMasterAddr()
				log.Printf("[Heartbeat %d] ⚠️ Master heartbeat timeout! Master was node %d (IP: %s). Last heartbeat %v ago (threshold: %v). Initiating re-election.",
					s.nodeID, oldMaster, oldMasterAddr, elapsed, s.cfg.HeartbeatTimeout())

				// Invalidate the connection to the old master.
				s.invalidatePeerConnection(oldMaster)

				// Clear the master ID so we don't skip monitoring while
				// the election is in progress. This forces the node to
				// know there is NO master until a new one is elected.
				s.masterID.Store(0)
				s.isMaster.Store(false)

				// Reset the heartbeat timer to prevent rapid-fire elections.
				s.lastHeartbeat.Store(time.Now().UnixNano())

				log.Printf("[Heartbeat %d] 🔄 Starting re-election after master %d (IP: %s) became unreachable",
					s.nodeID, oldMaster, oldMasterAddr)

				go s.startElection()
			}
		}
	}
}
