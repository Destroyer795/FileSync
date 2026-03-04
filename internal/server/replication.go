// =============================================================================
// Replication — Async Master → Follower File Replication
// =============================================================================
//
// After the master completes a write operation (Upload or Delete), it
// asynchronously replicates the file data and metadata to 1-2 follower
// nodes based on the replication_factor in config.yaml.
//
// Replication is NON-BLOCKING — the master does not wait for replication
// to complete before responding to the client. This gives us low-latency
// writes at the cost of eventual consistency on replicas.
//
// The ReplicateFile RPC streams file data in chunks from master → follower.
// For DELETE operations, only metadata (no data chunks) is sent.
// =============================================================================

package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/filesync/gen/filesync"
	"github.com/filesync/internal/metadata"
)

// replicateFile asynchronously pushes a file operation to follower replicas.
// This is called in a goroutine from executeUpload/executeDelete.
// Selects up to replication_factor follower nodes (excluding self).
func (s *Server) replicateFile(operation, filename string, data []byte, meta *metadata.FileMetadata) {
	startTime := time.Now()

	// Select target followers for replication.
	targets := s.selectReplicationTargets()
	if len(targets) == 0 {
		log.Printf("[Replication %d] No replication targets available", s.nodeID)
		return
	}

	log.Printf("[Replication %d] Replicating %s '%s' to %d followers",
		s.nodeID, operation, filename, len(targets))

	// Replicate to each target concurrently.
	for _, targetID := range targets {
		go func(tid int32) {
			replicateStart := time.Now()

			client, err := s.getPeerClient(tid)
			if err != nil {
				log.Printf("[Replication %d] Cannot connect to follower %d: %v",
					s.nodeID, tid, err)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			stream, err := client.ReplicateFile(ctx)
			if err != nil {
				log.Printf("[Replication %d] Failed to open replicate stream to %d: %v",
					s.nodeID, tid, err)
				return
			}

			// Build protobuf metadata.
			metaProto := &filesync.FileMetadataProto{
				Filename: meta.Filename,
				Version:  meta.Version,
				Size:     meta.Size,
				Checksum: meta.Checksum,
				Replicas: meta.Replicas,
			}

			if operation == "DELETE" {
				// For deletes, send a single message with metadata and operation.
				err = stream.Send(&filesync.ReplicateRequest{
					Metadata:  metaProto,
					Operation: "DELETE",
				})
				if err != nil {
					log.Printf("[Replication %d] Failed to send DELETE to %d: %v",
						s.nodeID, tid, err)
					return
				}
			} else {
				// For uploads, stream the file data in chunks.
				// First message: metadata + first chunk.
				firstChunkEnd := chunkSize
				if firstChunkEnd > len(data) {
					firstChunkEnd = len(data)
				}

				err = stream.Send(&filesync.ReplicateRequest{
					Metadata:  metaProto,
					ChunkData: data[:firstChunkEnd],
					Operation: "UPLOAD",
				})
				if err != nil {
					log.Printf("[Replication %d] Failed to send first chunk to %d: %v",
						s.nodeID, tid, err)
					return
				}

				// Subsequent chunks: data only.
				for offset := firstChunkEnd; offset < len(data); offset += chunkSize {
					end := offset + chunkSize
					if end > len(data) {
						end = len(data)
					}
					err = stream.Send(&filesync.ReplicateRequest{
						ChunkData: data[offset:end],
						Operation: "UPLOAD",
					})
					if err != nil {
						log.Printf("[Replication %d] Failed to send chunk to %d: %v",
							s.nodeID, tid, err)
						return
					}
				}
			}

			// Close the stream and get the response.
			resp, err := stream.CloseAndRecv()
			if err != nil {
				log.Printf("[Replication %d] Failed to close stream to %d: %v",
					s.nodeID, tid, err)
				return
			}

			elapsed := time.Since(replicateStart)
			if resp.Success {
				log.Printf("[Replication %d] ✅ Replicated '%s' to node %d (%v)",
					s.nodeID, filename, tid, elapsed)

				// Update the file index to include this replica.
				s.addReplica(filename, tid)
			} else {
				log.Printf("[Replication %d] ❌ Replication to node %d failed: %s",
					s.nodeID, tid, resp.Message)
			}
		}(targetID)
	}

	log.Printf("[Replication %d] Replication tasks dispatched (total setup: %v)",
		s.nodeID, time.Since(startTime))
}

// selectReplicationTargets picks up to replication_factor follower nodes
// to replicate to. Excludes the master (self).
func (s *Server) selectReplicationTargets() []int32 {
	allPeers := s.getAllPeerIDs()
	maxTargets := s.cfg.ReplicationFactor
	if maxTargets > len(allPeers) {
		maxTargets = len(allPeers)
	}

	// Simple selection: first N peers.
	targets := make([]int32, 0, maxTargets)
	for _, pid := range allPeers {
		if len(targets) >= maxTargets {
			break
		}
		targets = append(targets, pid)
	}
	return targets
}

// addReplica updates a file's metadata to include a new replica node.
func (s *Server) addReplica(filename string, nodeID int32) {
	meta := s.fileIndex.Get(filename)
	if meta == nil {
		return
	}

	// Check if already listed as a replica.
	for _, r := range meta.Replicas {
		if r == nodeID {
			return
		}
	}

	meta.Replicas = append(meta.Replicas, nodeID)
	s.fileIndex.Put(meta)
}

// ----------- ReplicateFile Handler (Follower Side) -----------

// ReplicateFile handles incoming replication streams from the master.
// The follower receives file data, writes it to local disk, and updates
// its local file index.
func (s *Server) ReplicateFile(stream filesync.FileSyncService_ReplicateFileServer) error {
	startTime := time.Now()

	var filename string
	var operation string
	var metaProto *filesync.FileMetadataProto
	var fileData []byte

	// Receive all chunks from the stream.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if req.Metadata != nil {
			metaProto = req.Metadata
			filename = req.Metadata.Filename
		}
		if req.Operation != "" {
			operation = req.Operation
		}
		if len(req.ChunkData) > 0 {
			fileData = append(fileData, req.ChunkData...)
		}
	}

	if filename == "" {
		return stream.SendAndClose(&filesync.ReplicateResponse{
			Success: false,
			Message: "No filename in replication request",
			NodeId:  s.nodeID,
		})
	}

	// Execute the replicated operation locally.
	switch operation {
	case "UPLOAD":
		// Write file to local disk.
		filePath := filepath.Join(s.dirs.Files, filename)
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return stream.SendAndClose(&filesync.ReplicateResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to create dir: %v", err),
				NodeId:  s.nodeID,
			})
		}
		if err := os.WriteFile(filePath, fileData, 0644); err != nil {
			return stream.SendAndClose(&filesync.ReplicateResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to write file: %v", err),
				NodeId:  s.nodeID,
			})
		}

		// Update local file index.
		meta := &metadata.FileMetadata{
			Filename:  metaProto.Filename,
			Version:   metaProto.Version,
			Size:      metaProto.Size,
			Checksum:  metaProto.Checksum,
			Replicas:  metaProto.Replicas,
			CreatedAt: metaProto.CreatedAt,
			UpdatedAt: metaProto.UpdatedAt,
		}
		s.fileIndex.Put(meta)

		log.Printf("[Replication %d] ✅ Received replica: '%s' v%d (%d bytes)",
			s.nodeID, filename, metaProto.Version, len(fileData))

	case "DELETE":
		// Remove file from local disk.
		filePath := filepath.Join(s.dirs.Files, filename)
		os.Remove(filePath)

		// Remove from local file index.
		s.fileIndex.Delete(filename)

		log.Printf("[Replication %d] ✅ Replicated DELETE: '%s'", s.nodeID, filename)

	default:
		return stream.SendAndClose(&filesync.ReplicateResponse{
			Success: false,
			Message: fmt.Sprintf("Unknown operation: %s", operation),
			NodeId:  s.nodeID,
		})
	}

	elapsed := time.Since(startTime).Milliseconds()
	return stream.SendAndClose(&filesync.ReplicateResponse{
		Success:           true,
		Message:           "Replication successful",
		NodeId:            s.nodeID,
		ReplicationTimeMs: elapsed,
	})
}
