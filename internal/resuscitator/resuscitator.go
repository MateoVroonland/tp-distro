package resuscitator

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/MateoVroonland/tp-distro/internal/env"
)

// Message types for bully algorithm
const (
	ELECTION    = "E"
	OK          = "K"
	COORDINATOR = "C"
	HEARTBEAT   = "H"
	ACK         = "A"
)

type Server struct {
	shuttingDown         bool
	services             []string
	nodeID               int
	totalNodes           int
	isLeader             bool
	electionInProgress   bool
	leaderLastSeen       time.Time
	mutex                sync.RWMutex
	electionMutex        sync.Mutex
	shutdownChan         chan struct{}
	udpConn              *net.UDPConn
	heartbeatCancel      chan struct{}
	heartbeatMutex       sync.Mutex
	leadershipChangeChan chan bool
	heartbeatACKs        map[int]chan bool
	ackMutex             sync.Mutex
	revivingNodes        map[int]bool
	revivingMutex        sync.Mutex
}

func NewServer(nodeID int, totalNodes int) *Server {

	var services []string
	for i := 1; i <= env.AppEnv.MOVIES_RECEIVER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("moviesreceiver_%d", i))
	}
	for i := 1; i <= env.AppEnv.RATINGS_RECEIVER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("ratingsreceiver_%d", i))
	}
	for i := 1; i <= env.AppEnv.RATINGS_JOINER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("ratingsjoiner_%d", i))
	}
	for i := 1; i <= env.AppEnv.CREDITS_JOINER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("credits_joiner_%d", i))
	}
	for i := 1; i <= env.AppEnv.CREDITS_RECEIVER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("credits_receiver_%d", i))
	}
	for i := 1; i <= env.AppEnv.BUDGET_REDUCER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("budget_reducer_%d", i))
	}
	for i := 1; i <= env.AppEnv.BUDGET_SINK_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("budget_sink_%d", i))
	}
	for i := 1; i <= env.AppEnv.CREDITS_SINK_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("credits_sink_%d", i))
	}
	for i := 1; i <= env.AppEnv.Q1_SINK_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("q1_sink_%d", i))
	}
	for i := 1; i <= env.AppEnv.Q3_SINK_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("q3_sink_%d", i))
	}
	for i := 1; i <= env.AppEnv.Q1_FILTER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("filter_q1_%d", i))
	}
	for i := 1; i <= env.AppEnv.Q3_FILTER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("filter_q3_%d", i))
	}
	for i := 1; i <= env.AppEnv.Q4_FILTER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("filter_q4_%d", i))
	}
	for i := 1; i <= env.AppEnv.SENTIMENT_WORKER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("sentiment_worker_%d", i))
	}
	for i := 1; i <= env.AppEnv.SENTIMENT_REDUCER_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("sentiment_reducer_%d", i))
	}
	for i := 1; i <= env.AppEnv.SENTIMENT_SINK_AMOUNT; i++ {
		services = append(services, fmt.Sprintf("sentiment_sink_%d", i))
	}
	return &Server{
		services:             services,
		nodeID:               nodeID,
		totalNodes:           totalNodes,
		isLeader:             false,
		electionInProgress:   false,
		leaderLastSeen:       time.Now(),
		shutdownChan:         make(chan struct{}),
		heartbeatCancel:      nil,
		heartbeatMutex:       sync.Mutex{},
		leadershipChangeChan: make(chan bool, 1),
		heartbeatACKs:        make(map[int]chan bool),
		ackMutex:             sync.Mutex{},
		revivingNodes:        make(map[int]bool),
		revivingMutex:        sync.Mutex{},
	}
}

func (s *Server) Start() {
	log.Printf("Node %d: Waiting for services to be ready...", s.nodeID)
	time.Sleep(5 * time.Second)

	s.startUDPListener()

	go s.startElection()

	go s.monitorLeader()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.shutdown()
	}()

	log.Printf("Node %d: Resuscitator started", s.nodeID)

	var serviceMonitorCancel chan struct{}

	for {
		select {
		case <-s.shutdownChan:
			log.Printf("Node %d: Shutting down...", s.nodeID)
			if serviceMonitorCancel != nil {
				close(serviceMonitorCancel)
			}
			return
		case isLeader := <-s.leadershipChangeChan:
			if isLeader && serviceMonitorCancel == nil {
				log.Printf("Node %d: Became leader, starting service monitoring", s.nodeID)
				serviceMonitorCancel = make(chan struct{})
				go s.monitorAllServices(serviceMonitorCancel)
			} else if !isLeader && serviceMonitorCancel != nil {
				log.Printf("Node %d: Lost leadership, stopping service monitoring", s.nodeID)
				close(serviceMonitorCancel)
				serviceMonitorCancel = nil
			}
		}
	}
}

func (s *Server) startUDPListener() {
	addr, err := net.ResolveUDPAddr("udp", ":8000")
	if err != nil {
		log.Printf("Node %d: Failed to resolve UDP address: %v", s.nodeID, err)
		return
	}

	s.udpConn, err = net.ListenUDP("udp", addr)

	if err != nil {
		log.Printf("Node %d: Failed to start UDP listener: %v", s.nodeID, err)
		return
	}

	log.Printf("Node %d: UDP listener started on port 8000", s.nodeID)

	go s.handleUDPMessages()
}

func (s *Server) handleUDPMessages() {
	buffer := make([]byte, 1024)

	for {
		select {
		case <-s.shutdownChan:
			return
		default:
			if s.udpConn == nil {
				return
			}

			s.udpConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, addr, err := s.udpConn.ReadFromUDP(buffer)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				log.Printf("Node %d: Error reading UDP message: %v", s.nodeID, err)
				continue
			}

			message := string(buffer[:n])
			s.processPeerMessage(message, addr)
		}
	}
}

func (s *Server) processPeerMessage(message string, addr *net.UDPAddr) {
	// Parse message format: TYPE_ID
	parts := strings.Split(message, "_")
	if len(parts) != 2 {
		log.Printf("Node %d: Invalid message format from %s: %s", s.nodeID, addr.String(), message)
		return
	}

	msgType := parts[0]
	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Printf("Node %d: Invalid node ID in message from %s: %s", s.nodeID, addr.String(), message)
		return
	}

	log.Printf("Node %d: Received %s message from node %d", s.nodeID, msgType, nodeID)

	switch msgType {
	case ELECTION:
		s.handleElectionMessage(nodeID)
	case OK:
		s.handleOKMessage(nodeID)
	case COORDINATOR:
		s.handleCoordinatorMessage(nodeID)
	case HEARTBEAT:
		s.handleHeartbeatMessage(nodeID)
	case ACK:
		s.handleACKMessage(nodeID)
	}
}

func (s *Server) handleElectionMessage(senderNodeID int) {
	// Always respond with OK if we have higher ID
	if s.nodeID > senderNodeID {
		response := fmt.Sprintf("%s_%d", OK, s.nodeID)
		s.sendUDPToPeer(senderNodeID, response)

		// Start our own election if not already in progress
		go s.startElection()
	}
}

func (s *Server) sendUDPToPeer(peerID int, message string) error {
	peerAddr := fmt.Sprintf("resuscitator_%d:8000", peerID)

	addr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		log.Printf("Node %d: Failed to resolve peer address %s: %v", s.nodeID, peerAddr, err)
		return err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("Node %d: Failed to create UDP connection to peer %d: %v", s.nodeID, peerID, err)
		return err
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.Write([]byte(message))
	if err != nil {
		log.Printf("Node %d: Failed to send UDP message to peer %d: %v", s.nodeID, peerID, err)
		return err
	}

	return nil
}

func (s *Server) handleOKMessage(senderNodeID int) {
	log.Printf("Node %d: Received OK from higher node %d, waiting for coordinator", s.nodeID, senderNodeID)
}

func (s *Server) handleCoordinatorMessage(leaderNodeID int) {
	s.mutex.Lock()
	wasLeader := s.isLeader
	s.isLeader = (leaderNodeID == s.nodeID)
	s.electionInProgress = false
	s.leaderLastSeen = time.Now()
	s.mutex.Unlock()

	if s.isLeader && !wasLeader {
		log.Printf("Node %d: I am the new leader!", s.nodeID)
		s.startHeartbeats()
		select {
		case s.leadershipChangeChan <- true:
		default:
			// Channel is full, skip notification
		}
	} else if !s.isLeader && wasLeader {
		log.Printf("Node %d: Node %d is the new leader, I lost leadership", s.nodeID, leaderNodeID)
		s.stopHeartbeats()

		// Clear reviving nodes tracking since we're no longer leader
		s.revivingMutex.Lock()
		s.revivingNodes = make(map[int]bool)
		s.revivingMutex.Unlock()

		select {
		case s.leadershipChangeChan <- false:
		default:
			// Channel is full, skip notification
		}
	} else if !s.isLeader {
		log.Printf("Node %d: Node %d is the new leader", s.nodeID, leaderNodeID)
	}
}

func (s *Server) handleHeartbeatMessage(leaderNodeID int) {
	s.mutex.Lock()
	s.leaderLastSeen = time.Now()
	log.Printf("Node %d: Received heartbeat from leader %d", s.nodeID, leaderNodeID)
	s.mutex.Unlock()

	// Send ACK response back to leader
	response := fmt.Sprintf("%s_%d", ACK, s.nodeID)
	err := s.sendUDPToPeer(leaderNodeID, response)
	if err != nil {
		log.Printf("Node %d: Failed to send ACK to leader %d: %v", s.nodeID, leaderNodeID, err)
	}
}

func (s *Server) handleACKMessage(senderNodeID int) {
	s.ackMutex.Lock()
	defer s.ackMutex.Unlock()

	if ackChan, exists := s.heartbeatACKs[senderNodeID]; exists {
		select {
		case ackChan <- true:
			log.Printf("Node %d: Received ACK from node %d", s.nodeID, senderNodeID)
		default:
			// Channel is full or closed, ignore
		}
	}
}

func (s *Server) startElection() {
	s.electionMutex.Lock()
	defer s.electionMutex.Unlock()

	if s.electionInProgress {
		return
	}

	s.electionInProgress = true
	log.Printf("Node %d: Starting election", s.nodeID)

	log.Printf("Node %d: Checking for existing leader before declaring myself...", s.nodeID)
	time.Sleep(3 * time.Second)

	s.mutex.RLock()
	recentHeartbeat := time.Since(s.leaderLastSeen) < 8*time.Second
	s.mutex.RUnlock()

	if recentHeartbeat {
		log.Printf("Node %d: Detected existing leader, aborting election", s.nodeID)
		s.electionInProgress = false
		return
	}

	// Send ELECTION message to all nodes with higher IDs
	higherNodeIDs := make([]int, 0)
	for i := s.nodeID + 1; i <= s.totalNodes; i++ {
		higherNodeIDs = append(higherNodeIDs, i)
	}

	if len(higherNodeIDs) == 0 {
		// No higher nodes, I am the leader
		s.becomeLeader()
		return
	}

	okReceived := false
	var wg sync.WaitGroup

	for _, peerID := range higherNodeIDs {
		wg.Add(1)
		go func(targetPeerID int) {
			defer wg.Done()
			if s.sendElectionMessage(targetPeerID) {
				okReceived = true
			}
		}(peerID)
	}

	wg.Wait()

	if !okReceived {
		// No OK received, I am the leader
		s.becomeLeader()
	} else {
		// Wait for coordinator message with timeout
		go func() {
			time.Sleep(5 * time.Second)
			s.mutex.Lock()
			if s.electionInProgress {
				s.electionInProgress = false
				s.mutex.Unlock()
				go s.startElection()
			} else {
				s.mutex.Unlock()
			}
		}()
	}
}

func (s *Server) sendElectionMessage(peerID int) bool {
	message := fmt.Sprintf("%s_%d", ELECTION, s.nodeID)
	err := s.sendUDPToPeer(peerID, message)
	if err != nil {
		log.Printf("Node %d: Failed to send ELECTION to peer %d: %v", s.nodeID, peerID, err)
		return false
	}

	// For UDP, we wait a short time for potential OK response
	// This is handled by the UDP message listener
	time.Sleep(500 * time.Millisecond)
	return true
}

func (s *Server) becomeLeader() {
	s.mutex.Lock()
	s.isLeader = true
	s.electionInProgress = false
	s.mutex.Unlock()

	log.Printf("Node %d: I am the new leader!", s.nodeID)

	// Send COORDINATOR message to all other nodes
	for i := 1; i <= s.totalNodes; i++ {
		if i != s.nodeID {
			go s.sendCoordinatorMessage(i)
		}
	}

	s.startHeartbeats()

	select {
	case s.leadershipChangeChan <- true:
	default:
	}
}

func (s *Server) startHeartbeats() {
	s.heartbeatMutex.Lock()
	defer s.heartbeatMutex.Unlock()

	// Stop any existing heartbeat goroutine
	if s.heartbeatCancel != nil {
		close(s.heartbeatCancel)
	}

	s.heartbeatCancel = make(chan struct{})

	// Start new heartbeat goroutine
	go s.sendHeartbeats(s.heartbeatCancel)
}

func (s *Server) stopHeartbeats() {
	s.heartbeatMutex.Lock()
	defer s.heartbeatMutex.Unlock()

	if s.heartbeatCancel != nil {
		close(s.heartbeatCancel)
		s.heartbeatCancel = nil
	}
}

func (s *Server) sendCoordinatorMessage(peerID int) {
	message := fmt.Sprintf("%s_%d", COORDINATOR, s.nodeID)
	err := s.sendUDPToPeer(peerID, message)
	if err != nil {
		log.Printf("Node %d: Failed to send COORDINATOR to peer %d: %v", s.nodeID, peerID, err)
	}
}

func (s *Server) sendHeartbeats(cancel chan struct{}) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	log.Printf("Node %d: Heartbeat goroutine started", s.nodeID)
	defer log.Printf("Node %d: Heartbeat goroutine stopped", s.nodeID)

	heartbeatCount := 0

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-cancel:
			return
		case <-ticker.C:
			s.mutex.RLock()
			isLeader := s.isLeader
			s.mutex.RUnlock()

			if !isLeader {
				return
			}

			heartbeatCount++
			if heartbeatCount%10 == 0 {
				log.Printf("Node %d: Sending heartbeats (round %d)", s.nodeID, heartbeatCount)
			}

			// Send heartbeat to all peer nodes
			for i := 1; i <= s.totalNodes; i++ {
				if i != s.nodeID {
					go s.sendHeartbeat(i)
				}
			}
		}
	}
}

func (s *Server) sendHeartbeat(peerID int) {
	// Create ACK channel for this peer
	s.ackMutex.Lock()
	ackChan := make(chan bool, 1)
	s.heartbeatACKs[peerID] = ackChan
	s.ackMutex.Unlock()

	// Try to send heartbeat message with retries
	message := fmt.Sprintf("%s_%d", HEARTBEAT, s.nodeID)
	heartbeatSent := false

	// Retry sending heartbeat up to 3 times
	for attempt := 1; attempt <= 3; attempt++ {
		err := s.sendUDPToPeer(peerID, message)
		if err == nil {
			heartbeatSent = true
			log.Printf("Node %d: Heartbeat sent to peer %d (attempt %d/3)", s.nodeID, peerID, attempt)
			break
		}

		if s.nodeID == 1 {
			log.Printf("Node %d: Failed to send heartbeat to peer %d (attempt %d/3): %v", s.nodeID, peerID, attempt, err)
		}

		// Wait a bit before retrying (except on last attempt)
		if attempt < 3 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	// If we couldn't send after 3 attempts, consider peer unreachable and proceed with revival
	if !heartbeatSent {
		if s.nodeID == 1 {
			log.Printf("Node %d: All heartbeat attempts to peer %d failed, considering peer unreachable", s.nodeID, peerID)
		}

		// Check if already being revived
		s.revivingMutex.Lock()
		alreadyReviving := s.revivingNodes[peerID]
		if !alreadyReviving {
			s.revivingNodes[peerID] = true
			s.revivingMutex.Unlock()

			log.Printf("Node %d: Peer %d unreachable after 3 attempts, attempting to revive", s.nodeID, peerID)
			go s.resurrectResuscitatorNode(peerID)
		} else {
			s.revivingMutex.Unlock()
			log.Printf("Node %d: Peer %d already being revived, skipping", s.nodeID, peerID)
		}

		// Clean up the ACK channel
		s.ackMutex.Lock()
		delete(s.heartbeatACKs, peerID)
		s.ackMutex.Unlock()
		return
	}

	// Wait for ACK response with timeout
	select {
	case <-ackChan:
		// ACK received, peer is alive
		log.Printf("Node %d: Peer %d is alive (ACK received)", s.nodeID, peerID)
	case <-time.After(5 * time.Second):
		// No ACK received after successful heartbeat send, consider peer down
		s.revivingMutex.Lock()
		alreadyReviving := s.revivingNodes[peerID]
		if !alreadyReviving {
			s.revivingNodes[peerID] = true
			s.revivingMutex.Unlock()

			log.Printf("Node %d: Peer %d did not respond to heartbeat, attempting to revive", s.nodeID, peerID)
			go s.resurrectResuscitatorNode(peerID)
		} else {
			s.revivingMutex.Unlock()
			log.Printf("Node %d: Peer %d already being revived, skipping", s.nodeID, peerID)
		}
	}

	// Clean up the ACK channel
	s.ackMutex.Lock()
	delete(s.heartbeatACKs, peerID)
	s.ackMutex.Unlock()
}

func (s *Server) monitorLeader() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			s.mutex.RLock()
			isLeader := s.isLeader
			lastSeen := s.leaderLastSeen
			s.mutex.RUnlock()

			if !isLeader && time.Since(lastSeen) > 10*time.Second {
				log.Printf("Node %d: Leader seems to be down, starting election", s.nodeID)
				go s.startElection()
			}
		}
	}
}

func (s *Server) shutdown() {
	s.shuttingDown = true
	close(s.shutdownChan)

	// Stop heartbeats
	s.stopHeartbeats()

	if s.udpConn != nil {
		s.udpConn.Close()
		s.udpConn = nil
	}
}

func (s *Server) monitorAllServices(cancel <-chan struct{}) {
	log.Printf("Node %d: Starting monitoring for all services...", s.nodeID)
	var wg sync.WaitGroup

	for _, serviceName := range s.services {
		wg.Add(1)
		go func(service string) {
			defer wg.Done()
			s.monitorService(service, cancel)
		}(serviceName)
	}

	wg.Wait()
	log.Printf("Node %d: Service monitoring stopped", s.nodeID)
}

func (s *Server) monitorService(serviceName string, cancel <-chan struct{}) {
	log.Printf("Node %d: Starting monitoring for service: %s", s.nodeID, serviceName)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cancel:
			log.Printf("Node %d: Stopping monitoring for service: %s (cancelled)", s.nodeID, serviceName)
			return
		case <-s.shutdownChan:
			log.Printf("Node %d: Stopping monitoring for service: %s (shutdown)", s.nodeID, serviceName)
			return
		case <-ticker.C:
			// Check if we're still the leader
			s.mutex.RLock()
			isLeader := s.isLeader
			s.mutex.RUnlock()

			if !isLeader {
				log.Printf("Node %d: No longer leader, stopping service monitoring for %s", s.nodeID, serviceName)
				return
			}

			var attemptsLeft = 3
			for attemptsLeft > 0 {
				if s.performHealthCheck(serviceName) {
					break
				}
				time.Sleep(500 * time.Millisecond)
				attemptsLeft--
			}

			if attemptsLeft == 0 {
				s.resurrectService(serviceName)
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func (s *Server) performHealthCheck(serviceName string) bool {
	serviceAddr := fmt.Sprintf("%s:7000", serviceName)

	// Create a new UDP connection for each health check
	conn, err := net.DialTimeout("udp", serviceAddr, 3*time.Second)
	if err != nil {
		log.Printf("Node %d: Service %s health check failed (UDP connection): %v", s.nodeID, serviceName, err)
		return false
	}
	defer conn.Close()

	// Set timeouts for the health check
	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	// Send PING message via UDP
	if _, err := conn.Write([]byte("PING")); err != nil {
		log.Printf("Node %d: Failed to send UDP health check to service %s: %v", s.nodeID, serviceName, err)
		return false
	}

	// Read response
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Printf("Node %d: Service %s UDP health check response failed: %v", s.nodeID, serviceName, err)
		return false
	}

	// Verify response content
	response := string(buffer[:n])
	expectedResponse := "PONG"
	if response != expectedResponse {
		log.Printf("Node %d: Service %s sent invalid response: expected '%s', got '%s'", s.nodeID, serviceName, expectedResponse, response)
		return false
	}

	log.Printf("Node %d: Service %s is healthy (UDP)", s.nodeID, serviceName)
	return true
}

func (s *Server) resurrectService(serviceName string) {
	log.Printf("Node %d: Attempting to resurrect service: %s", s.nodeID, serviceName)

	log.Printf("Node %d: Restarting Docker service: %s", s.nodeID, serviceName)

	if err := s.restartDockerService(serviceName); err != nil {
		log.Printf("Node %d: Docker-compose restart failed for service %s: %v", s.nodeID, serviceName, err)
		return
	}
	log.Printf("Node %d: Successfully restarted service %s via docker-compose", s.nodeID, serviceName)

	log.Printf("Node %d: Waiting for service %s to become ready...", s.nodeID, serviceName)
	time.Sleep(15 * time.Second)
}

func (s *Server) restartDockerService(serviceName string) error {
	workDir := "/app"

	// Use docker compose restart to restart only the specific service
	restartCmd := exec.Command("docker", "compose", "restart", serviceName)
	restartCmd.Dir = workDir

	// Capture output for debugging
	output, err := restartCmd.CombinedOutput()
	if err != nil {
		log.Printf("Node %d: Failed to restart service %s: %v, output: %s", s.nodeID, serviceName, err, string(output))
		return err
	}

	log.Printf("Node %d: Service %s restart output: %s", s.nodeID, serviceName, string(output))
	return nil
}

func (s *Server) resurrectResuscitatorNode(peerID int) {
	serviceName := fmt.Sprintf("resuscitator_%d", peerID)
	log.Printf("Node %d: Attempting to resurrect resuscitator node: %s", s.nodeID, serviceName)

	// Ensure we clean up the reviving tracking when done
	defer func() {
		s.revivingMutex.Lock()
		delete(s.revivingNodes, peerID)
		s.revivingMutex.Unlock()
		log.Printf("Node %d: Finished revival attempt for resuscitator node %s", s.nodeID, serviceName)
	}()

	if err := s.restartDockerService(serviceName); err != nil {
		log.Printf("Node %d: Failed to restart resuscitator node %s: %v", s.nodeID, serviceName, err)
		return
	}

	log.Printf("Node %d: Successfully restarted resuscitator node %s", s.nodeID, serviceName)

	// Wait for the resuscitator node to become ready
	log.Printf("Node %d: Waiting for resuscitator node %s to become ready...", s.nodeID, serviceName)
	time.Sleep(15 * time.Second)
}
