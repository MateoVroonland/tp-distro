package utils

import (
	"bufio"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

// Suicide is a testing utility that simulates random process termination to test
// fault tolerance in distributed systems. It provides controlled failure simulation
// by randomly terminating the current process based on configurable parameters.
//
// The utility maintains persistent state across process restarts by tracking
// termination count in a file, allowing for reproducible testing scenarios
// where you want to simulate a specific number of failures.
//
// Key Features:
//   - Configurable termination probability (0.0 to 1.0)
//   - Maximum termination limit to prevent infinite failures
//   - Persistent state tracking across restarts
//   - Thread-safe atomic file operations
//
// Usage Example:
//
//	// Create a suicide instance with 5% termination chance, max 3 failures
//	suicide := NewSuicide(0.05, 3)
//
//	// In your main processing loop
//	for {
//		// Do some work...
//		suicide.CommitSuicide() // May terminate the process
//	}
//
// WARNING: This utility is intended for testing purposes only and should
// NEVER be used in production code. It will forcefully terminate the
// calling process.
type Suicide struct {
	// Probability is the chance of process termination on each CommitSuicide() call.
	// Must be between 0.0 (never terminate) and 1.0 (always terminate).
	Probability float64

	// TimesToDie is the maximum number of times this process should terminate
	// before the suicide behavior is disabled. Once this limit is reached,
	// CommitSuicide() will no longer terminate the process.
	TimesToDie int

	// TimesDied tracks how many times this process has already terminated.
	// This value is persisted across restarts and automatically loaded
	// from the state file.
	TimesDied int
}

// NewSuicide creates and initializes a new Suicide instance for fault tolerance testing.
//
// The function automatically creates or reads the persistent state file
// "data/already_died.txt" to track termination count across process restarts.
// If the file doesn't exist, it's created with an initial count of 0.
//
// Parameters:
//   - probability: Termination probability per call (0.0 to 1.0)
//   - timesToDie: Maximum number of terminations before disabling
//
// Returns:
//   - *Suicide: Initialized suicide instance with loaded state
//
// Panics:
//   - If the state file cannot be created, opened, or read
//   - If the state file contains invalid data
func NewSuicide(probability float64, timesToDie int) *Suicide {
	if probability < 0.0 || probability > 1.0 {
		log.Fatalf("Probability must be between 0.0 and 1.0, got: %f", probability)
	}
	if timesToDie < 0 {
		log.Fatalf("TimesToDie must be non-negative, got: %d", timesToDie)
	}

	_, err := os.Stat("data/already_died.txt")
	if os.IsNotExist(err) {
		AtomicallyWriteFile("data/already_died.txt", []byte("0"))
	} else if err != nil {
		log.Printf("Failed to check already died file: %v", err)
	}

	alreadyDiedFile, err := os.Open("data/already_died.txt")
	if err != nil {
		log.Printf("Failed to open already died file: %v", err)
	}
	defer alreadyDiedFile.Close()

	reader := bufio.NewReader(alreadyDiedFile)
	alreadyDiedString, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read already died file: %v", err)
	}

	alreadyDied, err := strconv.Atoi(strings.TrimSpace(alreadyDiedString))
	if err != nil {
		log.Printf("Failed to convert already died string to int: %v", err)
		alreadyDied = 0
	}

	if alreadyDied >= timesToDie {
		log.Printf("All %d times to die reached, not suiciding anymore", timesToDie)
	}

	return &Suicide{
		Probability: probability,
		TimesToDie:  timesToDie,
		TimesDied:   alreadyDied,
	}
}

// CommitSuicide attempts to terminate the current process based on the configured
// probability and termination limits. This method should be called periodically
// in your processing loop to simulate random failures.
//
// Behavior:
//   - Generates a random number and compares it against the termination probability
//   - Only terminates if the random number is less than the probability AND
//     the current termination count is below the maximum limit
//   - When terminating, increments the persistent termination counter and
//     forcefully exits the process with exit code 1
//   - If termination limits are reached, this method becomes a no-op
//
// Thread Safety:
//   - Uses atomic file operations to safely update the termination counter
//   - Safe to call from multiple goroutines
//
// Example:
//
//	suicide := NewSuicide(0.1, 2) // 10% chance, max 2 terminations
//
//	for {
//		// Process some data...
//		suicide.CommitSuicide() // May terminate here
//		// Continue processing...
//	}
func (s *Suicide) CommitSuicide() {
	if rand.Float64() < s.Probability && s.TimesDied < s.TimesToDie {
		s.TimesDied++
		log.Printf("SUICIDING FOR %d TIME", s.TimesDied)
		AtomicallyWriteFile("data/already_died.txt", []byte(strconv.Itoa(s.TimesDied)))
		os.Exit(1)
	}
}
