package main

import (
	"log"
	"os"
	"strconv"

	"github.com/MateoVroonland/tp-distro/internal/chaosmonkey"
)

func main() {
	log.Println("Starting Chaos Monkey...")

	nodeIDStr := os.Getenv("ID")
	nodeID, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		log.Fatalf("Invalid node ID: %v", err)
	}

	minIntervalStr := os.Getenv("MIN_INTERVAL_SECONDS")
	minInterval, err := strconv.Atoi(minIntervalStr)
	if err != nil {
		log.Fatalf("Invalid min interval: %v", err)
	}

	maxIntervalStr := os.Getenv("MAX_INTERVAL_SECONDS")
	maxInterval, err := strconv.Atoi(maxIntervalStr)
	if err != nil {
		log.Fatalf("Invalid max interval: %v", err)
	}

	killProbabilityStr := os.Getenv("KILL_PROBABILITY")
	killProbability, err := strconv.ParseFloat(killProbabilityStr, 64)
	if err != nil {
		log.Fatalf("Invalid kill probability: %v", err)
	}

	if minInterval <= 0 {
		log.Fatalf("Min interval must be positive, got: %d", minInterval)
	}
	if maxInterval <= minInterval {
		log.Fatalf("Max interval (%d) must be greater than min interval (%d)", maxInterval, minInterval)
	}

	log.Printf("Chaos Monkey configuration:")
	log.Printf("  Node ID: %d", nodeID)
	log.Printf("  Min Interval: %d seconds", minInterval)
	log.Printf("  Max Interval: %d seconds", maxInterval)
	log.Printf("  Kill Probability: %.2f", killProbability)

	monkey := chaosmonkey.NewChaosMonkey(nodeID, minInterval, maxInterval, killProbability)

	monkey.Start()

	log.Println("Chaos Monkey stopped")
}
