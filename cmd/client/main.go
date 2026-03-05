// =============================================================================
// FileSync Client CLI
// =============================================================================
//
// A command-line client for interacting with the FileSync cluster.
//
// Supported commands:
//   upload    <filepath> [filename]  — Upload a file to the cluster
//   download  <filename> [outpath]   — Download a file from the cluster
//   list                             — List all files in the cluster
//   delete    <filename>             — Delete a file from the cluster
//   snapshot create [label]          — Create a cluster-wide snapshot
//   snapshot list                    — List all available snapshots
//   snapshot read <snapshot_id>      — Read a snapshot's file index
//
// Usage:
//   go run cmd/client/main.go -addr localhost:50051 upload myfile.txt
//   go run cmd/client/main.go -addr localhost:50051 list
//   go run cmd/client/main.go -addr localhost:50051 download myfile.txt
//   go run cmd/client/main.go -addr localhost:50051 snapshot create pre-deploy
// =============================================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/filesync/gen/filesync"
)

// chunkSize for streaming file uploads.
const chunkSize = 64 * 1024 // 64KB

func main() {
	// Parse flags.
	addr := flag.String("addr", "localhost:50051", "Address of the FileSync node (ip:port)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	// Connect to the FileSync node.
	conn, err := grpc.Dial(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", *addr, err)
	}
	defer conn.Close()

	client := filesync.NewFileSyncServiceClient(conn)

	// Route to the appropriate command handler.
	command := args[0]
	switch command {
	case "upload":
		handleUpload(client, args[1:])
	case "download":
		handleDownload(client, args[1:])
	case "list":
		handleList(client)
	case "delete":
		handleDelete(client, args[1:])
	case "snapshot":
		handleSnapshot(client, args[1:])
	case "leader":
		handleLeader(client)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

// ----------- Upload -----------

func handleUpload(client filesync.FileSyncServiceClient, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: upload <filepath> [remote_filename]")
		os.Exit(1)
	}

	localPath := args[0]
	remoteName := filepath.Base(localPath)
	if len(args) > 1 {
		remoteName = args[1]
	}

	// Read the local file.
	data, err := os.ReadFile(localPath)
	if err != nil {
		log.Fatalf("Failed to read file '%s': %v", localPath, err)
	}

	fmt.Printf("Uploading '%s' as '%s' (%d bytes)...\n", localPath, remoteName, len(data))
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := client.Upload(ctx)
	if err != nil {
		log.Fatalf("Failed to start upload stream: %v", err)
	}

	// Send metadata in the first chunk.
	firstEnd := chunkSize
	if firstEnd > len(data) {
		firstEnd = len(data)
	}

	err = stream.Send(&filesync.UploadRequest{
		Metadata: &filesync.FileMetadataProto{
			Filename: remoteName,
			Size:     int64(len(data)),
		},
		ChunkData: data[:firstEnd],
	})
	if err != nil {
		log.Fatalf("Failed to send first chunk: %v", err)
	}

	// Send remaining chunks.
	for offset := firstEnd; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		err = stream.Send(&filesync.UploadRequest{
			ChunkData: data[offset:end],
		})
		if err != nil {
			log.Fatalf("Failed to send chunk: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}

	elapsed := time.Since(startTime)
	if resp.Success {
		fmt.Printf("✅ Upload successful: '%s' v%d (%v, server: %dms)\n",
			remoteName, resp.Version, elapsed, resp.ProcessingTimeMs)
	} else {
		fmt.Printf("❌ Upload failed: %s\n", resp.Message)
	}
}

// ----------- Download -----------

func handleDownload(client filesync.FileSyncServiceClient, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: download <filename> [output_path]")
		os.Exit(1)
	}

	filename := args[0]
	outputPath := filename
	if len(args) > 1 {
		outputPath = args[1]
	}

	fmt.Printf("Downloading '%s'...\n", filename)
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := client.Download(ctx, &filesync.DownloadRequest{
		Filename: filename,
	})
	if err != nil {
		log.Fatalf("Failed to start download: %v", err)
	}

	var fileData []byte
	var meta *filesync.FileMetadataProto
	firstChunk := true

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Download error: %v", err)
		}

		if firstChunk && resp.Metadata != nil {
			meta = resp.Metadata
			firstChunk = false
		}

		fileData = append(fileData, resp.ChunkData...)
	}

	// Write to local file.
	if err := os.WriteFile(outputPath, fileData, 0644); err != nil {
		log.Fatalf("Failed to write file '%s': %v", outputPath, err)
	}

	elapsed := time.Since(startTime)
	if meta != nil {
		fmt.Printf("✅ Downloaded '%s' v%d (%d bytes, checksum: %s, %v)\n",
			filename, meta.Version, len(fileData), meta.Checksum, elapsed)
	} else {
		fmt.Printf("✅ Downloaded '%s' (%d bytes, %v)\n", filename, len(fileData), elapsed)
	}
}

// ----------- List -----------

func handleList(client filesync.FileSyncServiceClient) {
	fmt.Println("Listing files...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListFiles(ctx, &filesync.ListFilesRequest{})
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}

	if resp.TotalCount == 0 {
		fmt.Println("No files in the cluster.")
		return
	}

	fmt.Printf("\n%-30s %-8s %-12s %-64s %s\n",
		"FILENAME", "VERSION", "SIZE", "CHECKSUM", "REPLICAS")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────")

	for _, f := range resp.Files {
		fmt.Printf("%-30s v%-7d %-12s %-64s %v\n",
			f.Filename,
			f.Version,
			formatSize(f.Size),
			f.Checksum,
			f.Replicas,
		)
	}

	fmt.Printf("\nTotal: %d files (%dms query time)\n", resp.TotalCount, resp.QueryTimeMs)
}

// ----------- Delete -----------

func handleDelete(client filesync.FileSyncServiceClient, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: delete <filename>")
		os.Exit(1)
	}

	filename := args[0]
	fmt.Printf("Deleting '%s'...\n", filename)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Delete(ctx, &filesync.DeleteRequest{
		Filename: filename,
	})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	if resp.Success {
		fmt.Printf("✅ Deleted '%s' (%dms)\n", filename, resp.ProcessingTimeMs)
	} else {
		fmt.Printf("❌ Delete failed: %s\n", resp.Message)
	}
}

// ----------- Snapshot -----------

func handleSnapshot(client filesync.FileSyncServiceClient, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: snapshot <create|list|read> [args...]")
		os.Exit(1)
	}

	switch args[0] {
	case "create":
		label := ""
		if len(args) > 1 {
			label = args[1]
		}
		handleSnapshotCreate(client, label)

	case "list":
		handleSnapshotList(client)

	case "read":
		if len(args) < 2 {
			fmt.Println("Usage: snapshot read <snapshot_id>")
			os.Exit(1)
		}
		handleSnapshotRead(client, args[1])

	default:
		fmt.Printf("Unknown snapshot command: %s\n", args[0])
		fmt.Println("Usage: snapshot <create|list|read>")
		os.Exit(1)
	}
}

func handleSnapshotCreate(client filesync.FileSyncServiceClient, label string) {
	fmt.Printf("Creating snapshot (label: '%s')...\n", label)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.CreateSnapshot(ctx, &filesync.CreateSnapshotRequest{
		Label: label,
	})
	if err != nil {
		log.Fatalf("Snapshot create failed: %v", err)
	}

	if resp.Success {
		fmt.Printf("✅ Snapshot created: '%s' (%dms coordination)\n",
			resp.SnapshotId, resp.CoordinationTimeMs)
		if resp.SnapshotInfo != nil {
			fmt.Printf("   Files: %d, Size: %s, Nodes: %v\n",
				resp.SnapshotInfo.FileCount,
				formatSize(resp.SnapshotInfo.TotalSizeBytes),
				resp.SnapshotInfo.ParticipatingNodes,
			)
		}
	} else {
		fmt.Printf("❌ Snapshot failed: %s\n", resp.Message)
	}
}

func handleSnapshotList(client filesync.FileSyncServiceClient) {
	fmt.Println("Listing snapshots...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListSnapshots(ctx, &filesync.ListSnapshotsRequest{})
	if err != nil {
		log.Fatalf("Snapshot list failed: %v", err)
	}

	if resp.TotalCount == 0 {
		fmt.Println("No snapshots available.")
		return
	}

	fmt.Printf("\n%-35s %-15s %-8s %-12s %-20s %s\n",
		"SNAPSHOT ID", "LABEL", "FILES", "SIZE", "CREATED", "NODES")
	fmt.Println("───────────────────────────────────────────────────────────────────────────────────────────────────────────")

	for _, snap := range resp.Snapshots {
		created := time.Unix(0, snap.CreatedAt).Format("2006-01-02 15:04:05")
		fmt.Printf("%-35s %-15s %-8d %-12s %-20s %v\n",
			snap.SnapshotId,
			snap.Label,
			snap.FileCount,
			formatSize(snap.TotalSizeBytes),
			created,
			snap.ParticipatingNodes,
		)
	}

	fmt.Printf("\nTotal: %d snapshots\n", resp.TotalCount)
}

func handleSnapshotRead(client filesync.FileSyncServiceClient, snapshotID string) {
	fmt.Printf("Reading snapshot '%s'...\n", snapshotID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ReadSnapshot(ctx, &filesync.ReadSnapshotRequest{
		SnapshotId: snapshotID,
	})
	if err != nil {
		log.Fatalf("Snapshot read failed: %v", err)
	}

	if !resp.Success {
		fmt.Printf("❌ %s\n", resp.ErrorMessage)
		return
	}

	snap := resp.SnapshotInfo
	fmt.Printf("\nSnapshot: %s\n", snap.SnapshotId)
	fmt.Printf("Label:    %s\n", snap.Label)
	fmt.Printf("Created:  %s\n", time.Unix(0, snap.CreatedAt).Format("2006-01-02 15:04:05"))
	fmt.Printf("Files:    %d\n", snap.FileCount)
	fmt.Printf("Size:     %s\n", formatSize(snap.TotalSizeBytes))
	fmt.Printf("WAL Pos:  %d\n", snap.WalPosition)
	fmt.Printf("Term:     %d\n", snap.Term)
	fmt.Printf("Nodes:    %v\n\n", snap.ParticipatingNodes)

	if len(resp.FileIndex) > 0 {
		fmt.Printf("%-30s %-8s %-12s %-64s\n", "FILENAME", "VERSION", "SIZE", "CHECKSUM")
		fmt.Println("─────────────────────────────────────────────────────────────────────────────────────────────────────────────")
		for _, f := range resp.FileIndex {
			fmt.Printf("%-30s v%-7d %-12s %-64s\n",
				f.Filename, f.Version, formatSize(f.Size), f.Checksum)
		}
	}
}

// ----------- Helpers -----------

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// ----------- Leader Info -----------

func handleLeader(client filesync.FileSyncServiceClient) {
	fmt.Println("Querying current leader...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.GetLeaderInfo(ctx, &filesync.GetLeaderInfoRequest{})
	if err != nil {
		log.Fatalf("GetLeaderInfo failed: %v", err)
	}

	if resp.MasterNodeId == 0 {
		fmt.Println("⚠️  No leader currently elected.")
		return
	}

	fmt.Println()
	fmt.Printf("  Current Leader IP : %s\n", resp.MasterAddress)
	fmt.Printf("  Leader Node ID    : %d\n", resp.MasterNodeId)
	fmt.Printf("  Election Term     : %d\n", resp.Term)
	fmt.Printf("  Responding Node   : %d\n", resp.RespondingNodeId)
	if resp.IsSelfMaster {
		fmt.Println("  (This node is the current leader)")
	}
	fmt.Println()
}

func printUsage() {
	fmt.Println(`
FileSync CLI - Distributed File Sharing Client

Usage:
  filesync-cli [flags] <command> [args...]

Flags:
  -addr string    Address of the FileSync node (default "localhost:50051")

Commands:
  upload    <filepath> [remote_name]   Upload a file to the cluster
  download  <filename> [output_path]   Download a file from the cluster
  list                                 List all files in the cluster
  delete    <filename>                 Delete a file from the cluster
  leader                               Show current leader IP and info
  snapshot  create [label]             Create a cluster-wide snapshot
  snapshot  list                       List all available snapshots
  snapshot  read <snapshot_id>         Read a snapshot's file index

Examples:
  filesync-cli -addr 192.168.1.101:50051 upload ./report.pdf
  filesync-cli -addr 192.168.1.101:50051 list
  filesync-cli -addr 192.168.1.101:50051 download report.pdf ./local_copy.pdf
  filesync-cli -addr 192.168.1.101:50051 leader
  filesync-cli -addr 192.168.1.101:50051 snapshot create "before-update"
  filesync-cli -addr 192.168.1.101:50051 snapshot list
  filesync-cli -addr 192.168.1.101:50051 snapshot read snap_1709654400_1
`)
}
