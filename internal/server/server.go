// =============================================================================
// Package server — Core FileSync Server
// =============================================================================
//
// The Server struct is the central orchestrator for a FileSync node.
// It holds all state: node identity, master election state, file index,
// WAL, gRPC connections to peers, and timing configuration.
//
// The Server implements the FileSyncServiceServer gRPC interface.
// Individual handler methods are split across separate files:
//   - server.go     — struct definition, initialization, gRPC registration
//   - election.go   — Bully election & heartbeat logic
//   - fileops.go    — Upload, Download, Delete, ListFiles handlers
//   - replication.go — async file replication to followers
//   - checkpoint.go  — Koo-Toueg coordinated checkpointing
//   - snapshot.go    — snapshot creation, listing, reading
//   - recovery.go    — crash recovery from WAL + checkpoint
// =============================================================================

package server

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/filesync/gen/filesync"
	"github.com/filesync/internal/config"
	"github.com/filesync/internal/metadata"
	"github.com/filesync/internal/wal"
)

// Server is the core FileSync node. It manages all distributed system state
// and implements the FileSyncServiceServer gRPC interface.
type Server struct {
	// Embed the unimplemented server for forward compatibility.
	filesync.UnimplementedFileSyncServiceServer

	// ----------- Node Identity -----------

	// Unique integer ID for this node (from config, used for addressing).
	nodeID int32
	// Random election priority assigned at startup. Higher priority wins
	// Bully elections. This ensures the master is randomly selected rather
	// than always being the node with the highest config ID.
	electionPriority atomic.Int64
	// Whether this node is currently the elected master.
	isMaster atomic.Bool
	// The node ID of the current cluster master. Updated on election victory.
	masterID atomic.Int32

	// ----------- Cluster State -----------

	// Map of peer nodeID → gRPC client connection.
	// Lazily initialized on first use; protected by peersMu.
	// Connections are INVALIDATED when they enter a failed state.
	peers   map[int32]*grpc.ClientConn
	peersMu sync.RWMutex
	// Peer addresses from config (nodeID → "ip:port").
	peerAddrs map[int32]string
	// Map of peer nodeID → their election priority (learned during elections).
	peerPriorities   map[int32]int64
	peerPrioritiesMu sync.RWMutex

	// ----------- File Index -----------

	// Thread-safe in-memory file index (filename → FileMetadata).
	fileIndex *metadata.FileIndex

	// ----------- Snapshot Metadata -----------

	// In-memory list of snapshot metadata records. Protected by snapshotMu.
	snapshotMeta []*metadata.SnapshotInfo
	snapshotMu   sync.RWMutex

	// ----------- Write-Ahead Log -----------

	// Durable WAL backed by operations.jsonl.
	wal *wal.WAL

	// ----------- Checkpointing -----------

	// Counter: number of write operations since last checkpoint.
	// When this reaches checkpointInterval, the master triggers a checkpoint.
	opsSinceCheckpoint atomic.Int64
	// The latest globally-consistent checkpoint ID.
	checkpointID atomic.Int64

	// ----------- Heartbeat / Election -----------

	// Timestamp of the last heartbeat received from the master.
	// Used for failure detection (300ms timeout triggers election).
	lastHeartbeat atomic.Int64
	// Current election term/epoch. Monotonically increasing.
	term atomic.Int64
	// Whether an election is currently in progress (prevents duplicate elections).
	electionInProgress atomic.Bool

	// ----------- Centralized Mutual Exclusion -----------

	// The master's write lock. Only the master acquires this.
	// All write operations (Upload, Delete) are serialized through this mutex.
	// This implements the "Centralized Server" mutual exclusion algorithm.
	writeMu sync.Mutex

	// ----------- Configuration -----------

	// Loaded configuration from config.yaml.
	cfg *config.Config
	// Computed directory paths (files/, metadata/, snapshots/, wal/).
	dirs *config.DirPaths

	// ----------- gRPC Server -----------

	// The gRPC server instance for this node.
	grpcServer *grpc.Server

	// ----------- Lifecycle -----------

	// Context and cancel function for graceful shutdown.
	ctx    context.Context
	cancel context.CancelFunc
}

// NewServer creates and initializes a new FileSync server node.
// Reads config, creates directories, initializes WAL, loads file index,
// and assigns a random election priority for Bully elections.
func NewServer(cfg *config.Config) (*Server, error) {
	// Initialize local storage directories.
	dirs, err := config.InitDirectories(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init directories: %w", err)
	}

	// Initialize the Write-Ahead Log.
	walInstance, err := wal.NewWAL(dirs.WAL)
	if err != nil {
		return nil, fmt.Errorf("failed to init WAL: %w", err)
	}

	// Build peer address map from config.
	peerAddrs := make(map[int32]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peerAddrs[p.ID] = p.Address
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Generate a random election priority (higher wins Bully elections).
	// Uses current time as seed for uniqueness across nodes.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	priority := rng.Int63n(1_000_000) // Random number 0-999999.

	s := &Server{
		nodeID:         cfg.NodeID,
		peers:          make(map[int32]*grpc.ClientConn),
		peerAddrs:      peerAddrs,
		peerPriorities: make(map[int32]int64),
		fileIndex:      metadata.NewFileIndex(),
		wal:            walInstance,
		cfg:            cfg,
		dirs:           dirs,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Store the random election priority.
	s.electionPriority.Store(priority)

	log.Printf("[Server %d] 🎲 Random election priority: %d", cfg.NodeID, priority)

	// Set initial heartbeat timestamp to now (prevents immediate election on boot).
	s.lastHeartbeat.Store(time.Now().UnixNano())

	// Load existing file index from disk (if any).
	indexPath := fmt.Sprintf("%s/file_index.json", dirs.Metadata)
	if err := s.fileIndex.LoadFromFile(indexPath); err != nil {
		log.Printf("[Server %d] Warning: could not load file index: %v", cfg.NodeID, err)
	} else {
		log.Printf("[Server %d] Loaded file index with %d files", cfg.NodeID, s.fileIndex.Count())
	}

	return s, nil
}

// Start launches the gRPC server, performs crash recovery, initiates
// heartbeat monitoring, and begins the election check loop.
func (s *Server) Start() error {
	log.Printf("[Server %d] Starting FileSync node on port %d (priority: %d)...",
		s.nodeID, s.cfg.ListenPort, s.electionPriority.Load())

	// Perform crash recovery before accepting any RPCs.
	if err := s.recoverFromCrash(); err != nil {
		log.Printf("[Server %d] Warning: crash recovery encountered errors: %v", s.nodeID, err)
	}

	// Set up the gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.ListenPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.cfg.ListenPort, err)
	}

	s.grpcServer = grpc.NewServer()
	filesync.RegisterFileSyncServiceServer(s.grpcServer, s)

	// Start background goroutines for heartbeat monitoring and election.
	go s.heartbeatMonitorLoop()

	// Initially trigger an election to establish the master.
	go func() {
		// Brief delay to let all nodes come up.
		time.Sleep(2 * time.Second)
		s.startElection()
	}()

	log.Printf("[Server %d] gRPC server listening on :%d", s.nodeID, s.cfg.ListenPort)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully shuts down the server: stops gRPC, closes WAL, cancels context.
func (s *Server) Stop() {
	log.Printf("[Server %d] Shutting down...", s.nodeID)
	s.cancel()

	// Save the file index to disk before shutting down.
	indexPath := fmt.Sprintf("%s/file_index.json", s.dirs.Metadata)
	if err := s.fileIndex.SaveToFile(indexPath); err != nil {
		log.Printf("[Server %d] Warning: failed to save file index: %v", s.nodeID, err)
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.wal != nil {
		s.wal.Close()
	}

	// Close all peer connections.
	s.peersMu.Lock()
	for id, conn := range s.peers {
		conn.Close()
		log.Printf("[Server %d] Closed connection to peer %d", s.nodeID, id)
	}
	s.peersMu.Unlock()

	log.Printf("[Server %d] Shutdown complete.", s.nodeID)
}

// getPeerClient returns a gRPC client for the specified peer node.
// Creates a new connection if one doesn't exist or if the existing
// connection is in a failed/shutdown state (stale connection invalidation).
func (s *Server) getPeerClient(peerID int32) (filesync.FileSyncServiceClient, error) {
	s.peersMu.RLock()
	conn, ok := s.peers[peerID]
	s.peersMu.RUnlock()

	if ok {
		// Check connection health — invalidate stale connections!
		// This is the critical fix: when a peer dies, the cached connection
		// enters TransientFailure or Shutdown state. We must detect this and
		// create a fresh connection instead of endlessly returning errors.
		state := conn.GetState()
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			log.Printf("[Server %d] Stale connection to peer %d (state: %v), reconnecting...",
				s.nodeID, peerID, state)
			// Remove the stale connection.
			s.peersMu.Lock()
			conn.Close()
			delete(s.peers, peerID)
			s.peersMu.Unlock()
		} else {
			return filesync.NewFileSyncServiceClient(conn), nil
		}
	}

	// Need to create a new connection.
	addr, ok := s.peerAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID: %d", peerID)
	}

	s.peersMu.Lock()
	defer s.peersMu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it).
	if conn, ok := s.peers[peerID]; ok {
		state := conn.GetState()
		if state != connectivity.TransientFailure && state != connectivity.Shutdown {
			return filesync.NewFileSyncServiceClient(conn), nil
		}
		conn.Close()
		delete(s.peers, peerID)
	}

	// Dial the peer with a timeout. Use non-blocking mode so the dial
	// doesn't hang forever if the peer is down.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %d at %s: %w", peerID, addr, err)
	}

	s.peers[peerID] = conn
	log.Printf("[Server %d] Connected to peer %d at %s", s.nodeID, peerID, addr)
	return filesync.NewFileSyncServiceClient(conn), nil
}

// invalidatePeerConnection removes a cached peer connection, forcing
// a fresh reconnection on the next RPC attempt. Called when an RPC
// to a peer fails, indicating the connection may be broken.
func (s *Server) invalidatePeerConnection(peerID int32) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	if conn, ok := s.peers[peerID]; ok {
		conn.Close()
		delete(s.peers, peerID)
		log.Printf("[Server %d] Invalidated connection to peer %d", s.nodeID, peerID)
	}
}

// isMasterNode returns true if this node is the current master.
func (s *Server) isMasterNode() bool {
	return s.isMaster.Load()
}

// getMasterID returns the current master's node ID.
func (s *Server) getMasterID() int32 {
	return s.masterID.Load()
}

// getElectionPriority returns this node's random election priority.
func (s *Server) getElectionPriority() int64 {
	return s.electionPriority.Load()
}

// getPeerPriority returns a peer's last known election priority.
// Returns 0 if the peer's priority is unknown.
func (s *Server) getPeerPriority(peerID int32) int64 {
	s.peerPrioritiesMu.RLock()
	defer s.peerPrioritiesMu.RUnlock()
	return s.peerPriorities[peerID]
}

// setPeerPriority stores a peer's election priority.
func (s *Server) setPeerPriority(peerID int32, priority int64) {
	s.peerPrioritiesMu.Lock()
	defer s.peerPrioritiesMu.Unlock()
	s.peerPriorities[peerID] = priority
}

// getAllPeerIDs returns all configured peer IDs (excluding this node).
func (s *Server) getAllPeerIDs() []int32 {
	ids := make([]int32, 0, len(s.peerAddrs))
	for id := range s.peerAddrs {
		ids = append(ids, id)
	}
	return ids
}
