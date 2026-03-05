// =============================================================================
// Ricart-Agrawala Distributed Mutual Exclusion Algorithm
// =============================================================================
//
// Replaces the centralized server mutex (writeMu) with a fully distributed
// mutual exclusion algorithm. Any node can now enter the critical section
// (for writes) without forwarding to the master.
//
// Algorithm:
//   1. Node wanting CS increments its Lamport clock and multicasts
//      REQUEST(timestamp, nodeID) to ALL other nodes.
//   2. On receiving REQUEST, a node either:
//      a) Sends REPLY immediately — if it is not requesting CS, or if the
//         incoming request has higher priority (lower timestamp, or same
//         timestamp and lower nodeID).
//      b) DEFERS the reply — if it is in CS or requesting with higher priority.
//   3. The requesting node enters CS only after receiving REPLY from ALL peers.
//   4. On exiting CS, the node sends all deferred REPLY messages.
//
// Priority: Lower Lamport timestamp wins. Ties broken by lower node ID.
//
// Tagged with [R-A] in log messages for easy identification.
// =============================================================================

package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filesync/gen/filesync"
)

// RAState holds the Ricart-Agrawala algorithm state for a node.
type RAState struct {
	mu sync.Mutex

	// Lamport logical clock for event ordering.
	clock int64

	// Whether this node is currently requesting access to the CS.
	requesting bool
	// Whether this node is currently inside the CS.
	inCS bool

	// The Lamport timestamp of our current request (set when requesting).
	requestTS int64

	// Number of REPLY messages received for the current request.
	repliesReceived int
	// Total number of peers we need replies from.
	totalPeers int

	// Channel signaled when all replies have been received.
	replyCh chan struct{}

	// Queue of peer node IDs whose replies are deferred
	// (we'll send them when we exit the CS).
	deferredQueue []int32
}

// NewRAState creates a new Ricart-Agrawala state instance.
func NewRAState(totalPeers int) *RAState {
	return &RAState{
		totalPeers:    totalPeers,
		deferredQueue: make([]int32, 0),
		replyCh:       make(chan struct{}, 1),
	}
}

// tick increments the Lamport clock and returns the new value.
func (ra *RAState) tick() int64 {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.clock++
	return ra.clock
}

// updateClock updates the Lamport clock: max(local, received) + 1.
func (ra *RAState) updateClock(receivedTS int64) int64 {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if receivedTS > ra.clock {
		ra.clock = receivedTS
	}
	ra.clock++
	return ra.clock
}

// getClock returns the current Lamport clock value.
func (ra *RAState) getClock() int64 {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	return ra.clock
}

// requestCriticalSection sends REQUEST to all peers and blocks until
// all REPLY messages are received, granting access to the critical section.
func (s *Server) requestCriticalSection() error {
	allPeers := s.getAllPeerIDs()

	// If no peers, we can enter CS immediately (single-node cluster).
	if len(allPeers) == 0 {
		log.Printf("[R-A %d] No peers configured — entering CS immediately", s.nodeID)
		s.ra.mu.Lock()
		s.ra.inCS = true
		s.ra.mu.Unlock()
		return nil
	}

	// Step 1: Set our state to "requesting" and record our timestamp.
	ts := s.ra.tick()

	s.ra.mu.Lock()
	s.ra.requesting = true
	s.ra.requestTS = ts
	s.ra.repliesReceived = 0
	s.ra.totalPeers = len(allPeers)
	// Reset the reply channel.
	s.ra.replyCh = make(chan struct{}, 1)
	s.ra.mu.Unlock()

	log.Printf("[R-A %d] 🔐 Requesting CS — Lamport TS=%d, need %d replies",
		s.nodeID, ts, len(allPeers))

	// Step 2: Multicast REQUEST to all peers.
	var wg sync.WaitGroup
	var replyCount atomic.Int32

	for _, peerID := range allPeers {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			client, err := s.getPeerClient(pid)
			if err != nil {
				log.Printf("[R-A %d] ⚠️ Cannot reach peer %d for REQUEST: %v",
					s.nodeID, pid, err)
				// Count unreachable peers as implicit replies
				// (they can't contest the CS if they're down).
				count := replyCount.Add(1)
				s.ra.mu.Lock()
				s.ra.repliesReceived = int(count)
				if s.ra.repliesReceived >= s.ra.totalPeers {
					select {
					case s.ra.replyCh <- struct{}{}:
					default:
					}
				}
				s.ra.mu.Unlock()
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := client.MutexRequest(ctx, &filesync.MutexRequestMsg{
				NodeId:    s.nodeID,
				Timestamp: ts,
				Term:      s.term.Load(),
			})

			if err != nil {
				log.Printf("[R-A %d] ⚠️ MutexRequest to peer %d failed: %v (counting as reply)",
					s.nodeID, pid, err)
				s.invalidatePeerConnection(pid)
				// Treat failed peers as implicit replies.
				count := replyCount.Add(1)
				s.ra.mu.Lock()
				s.ra.repliesReceived = int(count)
				if s.ra.repliesReceived >= s.ra.totalPeers {
					select {
					case s.ra.replyCh <- struct{}{}:
					default:
					}
				}
				s.ra.mu.Unlock()
				return
			}

			// Got a REPLY!
			s.ra.updateClock(resp.Timestamp)
			count := replyCount.Add(1)
			log.Printf("[R-A %d] ✅ Received REPLY from peer %d (reply %d/%d)",
				s.nodeID, pid, count, len(allPeers))

			s.ra.mu.Lock()
			s.ra.repliesReceived = int(count)
			if s.ra.repliesReceived >= s.ra.totalPeers {
				select {
				case s.ra.replyCh <- struct{}{}:
				default:
				}
			}
			s.ra.mu.Unlock()
		}(peerID)
	}

	// Step 3: Wait for all replies (with a timeout).
	select {
	case <-s.ra.replyCh:
		log.Printf("[R-A %d] 🔓 All %d replies received — ENTERING critical section (TS=%d)",
			s.nodeID, len(allPeers), ts)
	case <-time.After(30 * time.Second):
		s.ra.mu.Lock()
		s.ra.requesting = false
		s.ra.mu.Unlock()
		return fmt.Errorf("timeout waiting for R-A replies (got %d/%d)",
			replyCount.Load(), len(allPeers))
	case <-s.ctx.Done():
		s.ra.mu.Lock()
		s.ra.requesting = false
		s.ra.mu.Unlock()
		return fmt.Errorf("context cancelled while waiting for R-A replies")
	}

	// Step 4: Enter CS.
	s.ra.mu.Lock()
	s.ra.inCS = true
	s.ra.requesting = false
	s.ra.mu.Unlock()

	return nil
}

// releaseCriticalSection exits the CS and sends all deferred REPLY messages.
func (s *Server) releaseCriticalSection() {
	s.ra.mu.Lock()
	s.ra.inCS = false
	s.ra.requesting = false
	deferred := make([]int32, len(s.ra.deferredQueue))
	copy(deferred, s.ra.deferredQueue)
	s.ra.deferredQueue = s.ra.deferredQueue[:0] // Clear the queue.
	s.ra.mu.Unlock()

	if len(deferred) > 0 {
		log.Printf("[R-A %d] 🔓 Exiting CS — sending %d deferred REPLY messages",
			s.nodeID, len(deferred))
	} else {
		log.Printf("[R-A %d] 🔓 Exiting CS — no deferred replies to send", s.nodeID)
	}

	// Send deferred replies.
	for _, peerID := range deferred {
		go func(pid int32) {
			client, err := s.getPeerClient(pid)
			if err != nil {
				log.Printf("[R-A %d] ⚠️ Cannot send deferred REPLY to peer %d: %v",
					s.nodeID, pid, err)
				return
			}

			ts := s.ra.tick()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err = client.MutexReply(ctx, &filesync.MutexReplyMsg{
				NodeId:    s.nodeID,
				Timestamp: ts,
			})
			if err != nil {
				log.Printf("[R-A %d] ⚠️ Deferred REPLY to peer %d failed: %v",
					s.nodeID, pid, err)
				s.invalidatePeerConnection(pid)
			} else {
				log.Printf("[R-A %d] 📤 Sent deferred REPLY to peer %d", s.nodeID, pid)
			}
		}(peerID)
	}
}

// MutexRequest handles incoming REQUEST messages from peers.
// Per Ricart-Agrawala:
//   - If we're NOT requesting and NOT in CS → reply immediately
//   - If we're IN CS → defer the reply
//   - If we're requesting → compare timestamps (lower wins; ties broken by nodeID)
func (s *Server) MutexRequest(ctx context.Context, req *filesync.MutexRequestMsg) (*filesync.MutexReplyMsg, error) {
	s.ra.updateClock(req.Timestamp)

	s.ra.mu.Lock()

	senderID := req.NodeId
	senderTS := req.Timestamp

	log.Printf("[R-A %d] 📥 Received REQUEST from node %d (TS=%d). My state: requesting=%v, inCS=%v, myTS=%d",
		s.nodeID, senderID, senderTS, s.ra.requesting, s.ra.inCS, s.ra.requestTS)

	shouldDefer := false

	if s.ra.inCS {
		// We're in the CS — MUST defer.
		shouldDefer = true
		log.Printf("[R-A %d] ⏳ DEFERRING reply to node %d (I'm in CS)", s.nodeID, senderID)
	} else if s.ra.requesting {
		// Both requesting — compare priorities.
		// Lower timestamp wins. If equal, lower node ID wins.
		if s.ra.requestTS < senderTS || (s.ra.requestTS == senderTS && s.nodeID < senderID) {
			// We have higher priority — defer.
			shouldDefer = true
			log.Printf("[R-A %d] ⏳ DEFERRING reply to node %d (my TS=%d < their TS=%d, or same TS and my ID=%d < their ID=%d)",
				s.nodeID, senderID, s.ra.requestTS, senderTS, s.nodeID, senderID)
		} else {
			// They have higher priority — reply immediately.
			log.Printf("[R-A %d] ✅ Replying IMMEDIATELY to node %d (their priority is higher)",
				s.nodeID, senderID)
		}
	} else {
		// Not requesting, not in CS — reply immediately.
		log.Printf("[R-A %d] ✅ Replying IMMEDIATELY to node %d (not interested in CS)",
			s.nodeID, senderID)
	}

	if shouldDefer {
		// Release the lock, then wait for CS exit by polling.
		// The RPC stays open, blocking the sender until we grant permission.
		s.ra.mu.Unlock()

		for {
			time.Sleep(10 * time.Millisecond)
			s.ra.mu.Lock()
			if !s.ra.inCS && !s.ra.requesting {
				s.ra.mu.Unlock()
				break
			}
			// Check if sender now has higher priority (we lost priority).
			if s.ra.requesting && (s.ra.requestTS > senderTS || (s.ra.requestTS == senderTS && s.nodeID > senderID)) {
				s.ra.mu.Unlock()
				break
			}
			s.ra.mu.Unlock()
		}

		log.Printf("[R-A %d] ✅ Deferred REPLY now granted to node %d", s.nodeID, senderID)

		ts := s.ra.tick()
		return &filesync.MutexReplyMsg{
			NodeId:    s.nodeID,
			Timestamp: ts,
		}, nil
	}

	// Reply immediately — release lock first.
	s.ra.mu.Unlock()

	ts := s.ra.tick()
	return &filesync.MutexReplyMsg{
		NodeId:    s.nodeID,
		Timestamp: ts,
	}, nil
}

// MutexReply handles incoming explicit REPLY messages from peers.
// This is used when a peer sends a deferred reply after exiting CS.
// In our gRPC implementation, replies are mostly handled inline via the
// MutexRequest RPC response, but this RPC handles the explicit deferred path.
func (s *Server) MutexReply(ctx context.Context, req *filesync.MutexReplyMsg) (*filesync.MutexReplyMsg, error) {
	s.ra.updateClock(req.Timestamp)

	log.Printf("[R-A %d] 📥 Received explicit REPLY from node %d", s.nodeID, req.NodeId)

	// Count this reply if we're still waiting.
	s.ra.mu.Lock()
	if s.ra.requesting {
		s.ra.repliesReceived++
		if s.ra.repliesReceived >= s.ra.totalPeers {
			select {
			case s.ra.replyCh <- struct{}{}:
			default:
			}
		}
	}
	s.ra.mu.Unlock()

	ts := s.ra.tick()
	return &filesync.MutexReplyMsg{
		NodeId:    s.nodeID,
		Timestamp: ts,
	}, nil
}
