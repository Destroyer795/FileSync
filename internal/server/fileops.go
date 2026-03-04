// =============================================================================
// File Operations — Upload, Download, Delete, ListFiles
// =============================================================================
//
// Implements the gRPC handlers for all file operations:
//
// WRITES (Upload, Delete):
//   - If this node is the master: acquire the centralized write lock (writeMu),
//     append to WAL, execute the operation, mark WAL committed, trigger
//     async replication, and check if a checkpoint is needed.
//   - If this node is a follower: forward the write request to the master
//     via the ForwardWrite RPC (centralized mutual exclusion pattern).
//
// READS (Download, ListFiles):
//   - Served locally from any node's replica — no master coordination needed.
//   - Target: < 5ms latency for local reads.
//
// The master's writeMu (sync.Mutex) is the centralized lock that serializes
// ALL write operations across the cluster, implementing the "Centralized
// Server" mutual exclusion algorithm.
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

// chunkSize is the size of each data chunk for streaming file transfers.
const chunkSize = 64 * 1024 // 64KB chunks

// ----------- Upload Handler -----------

// Upload handles client file uploads (streaming RPC).
// If this node is a follower, it collects all chunks and forwards
// the complete file to the master via ForwardWrite.
func (s *Server) Upload(stream filesync.FileSyncService_UploadServer) error {
	startTime := time.Now()
	log.Printf("[FileOps %d] Upload stream started", s.nodeID)

	// Collect all streamed chunks into a buffer.
	var filename string
	var fileData []byte
	var sourceNodeID int32

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive upload chunk: %w", err)
		}

		// Extract filename from the first message's metadata.
		if req.Metadata != nil && req.Metadata.Filename != "" {
			filename = req.Metadata.Filename
		}
		if req.SourceNodeId != 0 {
			sourceNodeID = req.SourceNodeId
		}

		// Append chunk data.
		if len(req.ChunkData) > 0 {
			fileData = append(fileData, req.ChunkData...)
		}
	}

	if filename == "" {
		return stream.SendAndClose(&filesync.UploadResponse{
			Success: false,
			Message: "No filename provided in upload metadata",
		})
	}

	log.Printf("[FileOps %d] Received file '%s' (%d bytes) from node %d",
		s.nodeID, filename, len(fileData), sourceNodeID)

	// If we are the master, process the write locally.
	if s.isMasterNode() {
		version, err := s.executeUpload(filename, fileData)
		elapsed := time.Since(startTime).Milliseconds()
		if err != nil {
			log.Printf("[FileOps %d] Upload failed for '%s': %v", s.nodeID, filename, err)
			return stream.SendAndClose(&filesync.UploadResponse{
				Success:          false,
				Message:          fmt.Sprintf("Upload failed: %v", err),
				ProcessingTimeMs: elapsed,
			})
		}
		log.Printf("[FileOps %d] ✅ Upload complete: '%s' v%d (%dms)",
			s.nodeID, filename, version, elapsed)
		return stream.SendAndClose(&filesync.UploadResponse{
			Success:          true,
			Message:          fmt.Sprintf("File '%s' uploaded successfully (v%d)", filename, version),
			Version:          version,
			ProcessingTimeMs: elapsed,
		})
	}

	// We are a follower — forward to master via ForwardWrite.
	log.Printf("[FileOps %d] Forwarding upload of '%s' to master %d",
		s.nodeID, filename, s.getMasterID())

	resp, err := s.forwardWriteToMaster("UPLOAD", filename, fileData)
	elapsed := time.Since(startTime).Milliseconds()
	if err != nil {
		return stream.SendAndClose(&filesync.UploadResponse{
			Success:          false,
			Message:          fmt.Sprintf("Forward to master failed: %v", err),
			ProcessingTimeMs: elapsed,
		})
	}

	return stream.SendAndClose(&filesync.UploadResponse{
		Success:          resp.Success,
		Message:          resp.Message,
		Version:          resp.Version,
		ProcessingTimeMs: elapsed,
	})
}

// executeUpload is the master's internal upload handler.
// Acquires the centralized write lock, appends to WAL, writes to disk,
// updates the file index, and triggers async replication.
func (s *Server) executeUpload(filename string, data []byte) (int64, error) {
	// Step 1: Acquire the centralized write lock.
	// This is the Centralized Server mutual exclusion — single-threaded writes.
	lockStart := time.Now()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	lockWait := time.Since(lockStart)
	log.Printf("[FileOps %d] Write lock acquired for '%s' (waited %v)",
		s.nodeID, filename, lockWait)

	// Step 2: Determine the new version number.
	existing := s.fileIndex.Get(filename)
	var version int64 = 1
	if existing != nil {
		version = existing.Version + 1
	}

	// Step 3: Append to WAL BEFORE executing the operation.
	entryID, err := s.wal.Append("UPLOAD", filename, version)
	if err != nil {
		return 0, fmt.Errorf("WAL append failed: %w", err)
	}
	log.Printf("[FileOps %d] WAL entry %d: UPLOAD '%s' v%d", s.nodeID, entryID, filename, version)

	// Step 4: Write the file data to local disk.
	filePath := filepath.Join(s.dirs.Files, filename)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return 0, fmt.Errorf("failed to create file directory: %w", err)
	}
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return 0, fmt.Errorf("failed to write file data: %w", err)
	}

	// Step 5: Update the in-memory file index.
	meta := metadata.NewFileMetadata(filename, data, s.nodeID)
	meta.Version = version
	if existing != nil {
		meta.CreatedAt = existing.CreatedAt // Preserve original creation time.
	}
	s.fileIndex.Put(meta)

	// Step 6: Mark the WAL entry as committed.
	if err := s.wal.MarkCommitted(entryID); err != nil {
		log.Printf("[FileOps %d] Warning: failed to mark WAL entry %d committed: %v",
			s.nodeID, entryID, err)
	}

	// Step 7: Trigger async replication to followers (non-blocking).
	go s.replicateFile("UPLOAD", filename, data, meta)

	// Step 8: Check if we need to trigger a checkpoint.
	opsCount := s.opsSinceCheckpoint.Add(1)
	if int(opsCount) >= s.cfg.CheckpointInterval {
		go s.triggerCheckpoint()
	}

	return version, nil
}

// ----------- Download Handler -----------

// Download streams a file back to the requesting client.
// Served locally — no master coordination needed for reads.
func (s *Server) Download(req *filesync.DownloadRequest, stream filesync.FileSyncService_DownloadServer) error {
	startTime := time.Now()
	log.Printf("[FileOps %d] Download request for '%s' from node %d",
		s.nodeID, req.Filename, req.RequestingNodeId)

	// Look up file metadata from the local index.
	meta := s.fileIndex.Get(req.Filename)
	if meta == nil {
		return fmt.Errorf("file '%s' not found in index", req.Filename)
	}

	// Read the file from local disk.
	filePath := filepath.Join(s.dirs.Files, req.Filename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file '%s': %w", req.Filename, err)
	}

	// Send the first chunk with metadata.
	firstChunkEnd := chunkSize
	if firstChunkEnd > len(data) {
		firstChunkEnd = len(data)
	}

	err = stream.Send(&filesync.DownloadResponse{
		Metadata: &filesync.FileMetadataProto{
			Filename:  meta.Filename,
			Version:   meta.Version,
			Size:      meta.Size,
			Checksum:  meta.Checksum,
			Replicas:  meta.Replicas,
			CreatedAt: meta.CreatedAt,
			UpdatedAt: meta.UpdatedAt,
		},
		ChunkData:      data[:firstChunkEnd],
		BytesRemaining: int64(len(data) - firstChunkEnd),
	})
	if err != nil {
		return fmt.Errorf("failed to send first download chunk: %w", err)
	}

	// Stream remaining chunks.
	for offset := firstChunkEnd; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}

		err = stream.Send(&filesync.DownloadResponse{
			ChunkData:      data[offset:end],
			BytesRemaining: int64(len(data) - end),
		})
		if err != nil {
			return fmt.Errorf("failed to send download chunk: %w", err)
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("[FileOps %d] ✅ Download complete: '%s' (%d bytes, %v)",
		s.nodeID, req.Filename, len(data), elapsed)

	return nil
}

// ----------- Delete Handler -----------

// Delete removes a file from the cluster. Write operation — follows the
// centralized mutex path (forwarded to master if called on a follower).
func (s *Server) Delete(ctx context.Context, req *filesync.DeleteRequest) (*filesync.DeleteResponse, error) {
	startTime := time.Now()
	log.Printf("[FileOps %d] Delete request for '%s' from node %d",
		s.nodeID, req.Filename, req.SourceNodeId)

	// If we are the master, process locally.
	if s.isMasterNode() {
		err := s.executeDelete(req.Filename)
		elapsed := time.Since(startTime).Milliseconds()
		if err != nil {
			return &filesync.DeleteResponse{
				Success:          false,
				Message:          fmt.Sprintf("Delete failed: %v", err),
				ProcessingTimeMs: elapsed,
			}, nil
		}
		return &filesync.DeleteResponse{
			Success:          true,
			Message:          fmt.Sprintf("File '%s' deleted successfully", req.Filename),
			ProcessingTimeMs: elapsed,
		}, nil
	}

	// Forward to master.
	resp, err := s.forwardWriteToMaster("DELETE", req.Filename, nil)
	elapsed := time.Since(startTime).Milliseconds()
	if err != nil {
		return &filesync.DeleteResponse{
			Success:          false,
			Message:          fmt.Sprintf("Forward to master failed: %v", err),
			ProcessingTimeMs: elapsed,
		}, nil
	}

	return &filesync.DeleteResponse{
		Success:          resp.Success,
		Message:          resp.Message,
		ProcessingTimeMs: elapsed,
	}, nil
}

// executeDelete is the master's internal delete handler.
func (s *Server) executeDelete(filename string) error {
	// Acquire the centralized write lock.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Check if file exists.
	meta := s.fileIndex.Get(filename)
	if meta == nil {
		return fmt.Errorf("file '%s' not found", filename)
	}

	// Append DELETE to WAL BEFORE executing.
	entryID, err := s.wal.Append("DELETE", filename, meta.Version)
	if err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}

	// Remove from local disk.
	filePath := filepath.Join(s.dirs.Files, filename)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		log.Printf("[FileOps %d] Warning: failed to remove file '%s': %v", s.nodeID, filename, err)
	}

	// Remove from file index.
	s.fileIndex.Delete(filename)

	// Mark WAL entry committed.
	if err := s.wal.MarkCommitted(entryID); err != nil {
		log.Printf("[FileOps %d] Warning: failed to mark WAL entry %d committed: %v",
			s.nodeID, entryID, err)
	}

	// Trigger async replication of the delete to followers.
	go s.replicateFile("DELETE", filename, nil, meta)

	// Checkpoint check.
	opsCount := s.opsSinceCheckpoint.Add(1)
	if int(opsCount) >= s.cfg.CheckpointInterval {
		go s.triggerCheckpoint()
	}

	log.Printf("[FileOps %d] ✅ Delete complete: '%s'", s.nodeID, filename)
	return nil
}

// ----------- ListFiles Handler -----------

// ListFiles returns metadata for all files in the local index.
// Read-only — served locally from any node.
func (s *Server) ListFiles(ctx context.Context, req *filesync.ListFilesRequest) (*filesync.ListFilesResponse, error) {
	startTime := time.Now()

	files := s.fileIndex.List()

	// Convert to protobuf format.
	protoFiles := make([]*filesync.FileMetadataProto, 0, len(files))
	for _, f := range files {
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

	elapsed := time.Since(startTime).Milliseconds()
	log.Printf("[FileOps %d] ListFiles: %d files (%dms)", s.nodeID, len(protoFiles), elapsed)

	return &filesync.ListFilesResponse{
		Files:       protoFiles,
		TotalCount:  int32(len(protoFiles)),
		QueryTimeMs: elapsed,
	}, nil
}

// ----------- ForwardWrite Handler -----------

// ForwardWrite handles write requests forwarded from follower nodes.
// This is the master-side of the centralized mutual exclusion algorithm.
// Only the master receives and processes these requests.
func (s *Server) ForwardWrite(ctx context.Context, req *filesync.ForwardWriteRequest) (*filesync.ForwardWriteResponse, error) {
	startTime := time.Now()
	log.Printf("[FileOps %d] ForwardWrite received: op=%s file='%s' from node %d",
		s.nodeID, req.Operation, req.Filename, req.SourceNodeId)

	if !s.isMasterNode() {
		return &filesync.ForwardWriteResponse{
			Success: false,
			Message: "This node is not the master",
		}, nil
	}

	switch req.Operation {
	case "UPLOAD":
		version, err := s.executeUpload(req.Filename, req.FileData)
		elapsed := time.Since(startTime).Milliseconds()
		if err != nil {
			return &filesync.ForwardWriteResponse{
				Success:          false,
				Message:          fmt.Sprintf("Upload failed: %v", err),
				ProcessingTimeMs: elapsed,
			}, nil
		}
		return &filesync.ForwardWriteResponse{
			Success:          true,
			Message:          fmt.Sprintf("File '%s' uploaded (v%d) via forward", req.Filename, version),
			Version:          version,
			ProcessingTimeMs: elapsed,
		}, nil

	case "DELETE":
		err := s.executeDelete(req.Filename)
		elapsed := time.Since(startTime).Milliseconds()
		if err != nil {
			return &filesync.ForwardWriteResponse{
				Success:          false,
				Message:          fmt.Sprintf("Delete failed: %v", err),
				ProcessingTimeMs: elapsed,
			}, nil
		}
		return &filesync.ForwardWriteResponse{
			Success:          true,
			Message:          fmt.Sprintf("File '%s' deleted via forward", req.Filename),
			ProcessingTimeMs: elapsed,
		}, nil

	default:
		return &filesync.ForwardWriteResponse{
			Success: false,
			Message: fmt.Sprintf("Unknown operation: %s", req.Operation),
		}, nil
	}
}

// forwardWriteToMaster sends a write operation to the current master.
func (s *Server) forwardWriteToMaster(operation, filename string, data []byte) (*filesync.ForwardWriteResponse, error) {
	masterID := s.getMasterID()
	if masterID == 0 {
		return nil, fmt.Errorf("no master elected")
	}

	client, err := s.getPeerClient(masterID)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to master %d: %w", masterID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var metaProto *filesync.FileMetadataProto
	if operation == "UPLOAD" && data != nil {
		meta := metadata.NewFileMetadata(filename, data, s.nodeID)
		metaProto = &filesync.FileMetadataProto{
			Filename: meta.Filename,
			Size:     meta.Size,
			Checksum: meta.Checksum,
		}
	}

	return client.ForwardWrite(ctx, &filesync.ForwardWriteRequest{
		Operation:    operation,
		Filename:     filename,
		FileData:     data,
		SourceNodeId: s.nodeID,
		Metadata:     metaProto,
		ForwardedAt:  time.Now().UnixNano(),
	})
}
