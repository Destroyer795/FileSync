// =============================================================================
// FileSync Server Entry Point
// =============================================================================
//
// Bootstraps a FileSync node:
//   1. Parses command-line flags for config path
//   2. Loads config.yaml
//   3. Creates and starts the Server
//   4. Handles graceful shutdown on SIGINT/SIGTERM
//
// Usage:
//   go run cmd/filesync/main.go -config config.yaml
//   go run cmd/filesync/main.go -config node2_config.yaml
// =============================================================================

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/filesync/internal/config"
	"github.com/filesync/internal/server"
)

func main() {
	// Parse command-line flags.
	configPath := flag.String("config", "config.yaml", "Path to the configuration YAML file")
	flag.Parse()

	// Configure structured logging with timestamps.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Printf("[Main] FileSync node starting, config: %s", *configPath)

	// Load configuration.
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("[Main] Failed to load config: %v", err)
	}
	log.Printf("[Main] Config loaded: NodeID=%d, Port=%d, Peers=%d, DataDir=%s",
		cfg.NodeID, cfg.ListenPort, len(cfg.Peers), cfg.DataDir)

	// Create the server.
	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("[Main] Failed to create server: %v", err)
	}

	// Handle graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("[Main] Received signal %v, shutting down...", sig)
		srv.Stop()
		os.Exit(0)
	}()

	// Start the server (blocks until shutdown).
	if err := srv.Start(); err != nil {
		log.Fatalf("[Main] Server failed: %v", err)
	}
}
