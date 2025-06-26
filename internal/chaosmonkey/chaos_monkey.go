package chaosmonkey

import (
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ChaosMonkey struct {
	services        []string
	nodeID          int
	isActive        bool
	shutdownChan    chan struct{}
	mutex           sync.RWMutex
	minInterval     time.Duration
	maxInterval     time.Duration
	killProbability float64
}

func NewChaosMonkey(nodeID int, minIntervalSeconds int, maxIntervalSeconds int, killProbability float64) *ChaosMonkey {
	services := []string{
		"moviesreceiver_1",
		"moviesreceiver_2",
		"filter_q1_1",
		"filter_q1_2",
		"filter_q3_1",
		"filter_q3_2",
		"filter_q4_1",
		"filter_q4_2",
		"ratingsreceiver_1",
		"ratingsreceiver_2",
		"ratingsjoiner_1",
		"ratingsjoiner_2",
		"q1_sink_1",
		"q3_sink_1",
		"budget_reducer_1",
		"budget_reducer_2",
		"budget_sink_1",
		"credits_joiner_1",
		"credits_joiner_2",
		"credits_receiver_1",
		"credits_receiver_2",
		"credits_sink_1",
		"resuscitator_1",
		"resuscitator_2",
		"resuscitator_3",
		"sentiment_worker_1",
		"sentiment_worker_2",
		"sentiment_sink_1",
		"sentiment_reducer_1",
	}

	// Validate probability
	if killProbability < 0 {
		killProbability = 0
	} else if killProbability > 1 {
		killProbability = 1
	}

	return &ChaosMonkey{
		services:        services,
		nodeID:          nodeID,
		isActive:        false,
		shutdownChan:    make(chan struct{}),
		minInterval:     time.Duration(minIntervalSeconds) * time.Second,
		maxInterval:     time.Duration(maxIntervalSeconds) * time.Second,
		killProbability: killProbability,
	}
}

func (cm *ChaosMonkey) Start() {
	log.Printf("Chaos Monkey %d: Starting chaos monkey with kill probability %.2f", cm.nodeID, cm.killProbability)
	log.Printf("Chaos Monkey %d: Interval range: %v to %v", cm.nodeID, cm.minInterval, cm.maxInterval)

	log.Printf("Chaos Monkey %d: Waiting 8 seconds for services to stabilize...", cm.nodeID)
	time.Sleep(8 * time.Second)

	cm.mutex.Lock()
	cm.isActive = true
	cm.mutex.Unlock()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		cm.shutdown()
	}()

	go cm.chaosLoop()

	log.Printf("Chaos Monkey %d: Chaos monkey started and active", cm.nodeID)

	<-cm.shutdownChan
	log.Printf("Chaos Monkey %d: Chaos monkey shutting down...", cm.nodeID)
}

func (cm *ChaosMonkey) chaosLoop() {
	for {
		select {
		case <-cm.shutdownChan:
			return
		default:
			cm.mutex.RLock()
			active := cm.isActive
			cm.mutex.RUnlock()

			if !active {
				time.Sleep(1 * time.Second)
				continue
			}

			intervalRange := cm.maxInterval - cm.minInterval
			randomInterval := cm.minInterval + time.Duration(rand.Int63n(int64(intervalRange)))

			log.Printf("Chaos Monkey %d: Next chaos event in %v", cm.nodeID, randomInterval)

			select {
			case <-cm.shutdownChan:
				return
			case <-time.After(randomInterval):
				cm.executeChaoticAction()
			}
		}
	}
}

func (cm *ChaosMonkey) executeChaoticAction() {
	if rand.Float64() > cm.killProbability {
		log.Printf("Chaos Monkey %d: Skipping chaos event (probability check failed)", cm.nodeID)
		return
	}

	if len(cm.services) == 0 {
		log.Printf("Chaos Monkey %d: No services available for chaos", cm.nodeID)
		return
	}

	targetService := cm.services[rand.Intn(len(cm.services))]

	log.Printf("CHAOS EVENT: Killing service %s", targetService)

	if err := cm.killService(targetService); err != nil {
		log.Printf("Chaos Monkey %d: Failed to kill service %s: %v", cm.nodeID, targetService, err)
	} else {
		log.Printf("Chaos Monkey %d: Successfully killed service %s", cm.nodeID, targetService)
	}
}

func (cm *ChaosMonkey) killService(serviceName string) error {
	workDir := "/app"

	killCmd := exec.Command("docker", "compose", "kill", serviceName)
	killCmd.Dir = workDir

	output, err := killCmd.CombinedOutput()
	if err != nil {
		log.Printf("Chaos Monkey %d: Failed to kill service %s: %v, output: %s", cm.nodeID, serviceName, err, string(output))
		return err
	}

	log.Printf("Chaos Monkey %d: Service %s kill output: %s", cm.nodeID, serviceName, string(output))
	return nil
}

func (cm *ChaosMonkey) Stop() {
	cm.mutex.Lock()
	cm.isActive = false
	cm.mutex.Unlock()

	log.Printf("Chaos Monkey %d: Chaos monkey stopped", cm.nodeID)
}

func (cm *ChaosMonkey) Resume() {
	cm.mutex.Lock()
	cm.isActive = true
	cm.mutex.Unlock()

	log.Printf("Chaos Monkey %d: Chaos monkey resumed", cm.nodeID)
}

func (cm *ChaosMonkey) IsActive() bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.isActive
}

func (cm *ChaosMonkey) shutdown() {
	cm.mutex.Lock()
	cm.isActive = false
	cm.mutex.Unlock()

	close(cm.shutdownChan)
}

func (cm *ChaosMonkey) GetStats() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return map[string]interface{}{
		"node_id":          cm.nodeID,
		"is_active":        cm.isActive,
		"services_count":   len(cm.services),
		"min_interval":     cm.minInterval.String(),
		"max_interval":     cm.maxInterval.String(),
		"kill_probability": cm.killProbability,
	}
}
