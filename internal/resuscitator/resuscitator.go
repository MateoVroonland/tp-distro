package resuscitator

import (
	"bufio"
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
)

// Message types for bully algorithm
const (
	ELECTION    = "ELECTION"
	OK          = "OK"
	COORDINATOR = "COORDINATOR"
	HEARTBEAT   = "HEARTBEAT"
)

// Connection holds a persistent connection with metadata
type Connection struct {
	conn      net.Conn
	addr      string
	lastUsed  time.Time
	isHealthy bool
	mutex     sync.RWMutex
}

// ConnectionManager manages persistent connections
type ConnectionManager struct {
	connections map[string]*Connection
	mutex       sync.RWMutex
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]*Connection),
	}
}

func (cm *ConnectionManager) getConnection(addr string) (*Connection, error) {
	cm.mutex.RLock()
	conn, exists := cm.connections[addr]
	cm.mutex.RUnlock()

	if exists && conn.isHealthy {
		conn.mutex.Lock()
		conn.lastUsed = time.Now()
		conn.mutex.Unlock()
		return conn, nil
	}

	// Create new connection
	return cm.createConnection(addr)
}

func (cm *ConnectionManager) createConnection(addr string) (*Connection, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Double-check after acquiring lock
	if conn, exists := cm.connections[addr]; exists && conn.isHealthy {
		return conn, nil
	}

	// Create new TCP connection with keep-alive
	tcpConn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	// Enable keep-alive
	if tcpConn, ok := tcpConn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	conn := &Connection{
		conn:      tcpConn,
		addr:      addr,
		lastUsed:  time.Now(),
		isHealthy: true,
	}

	cm.connections[addr] = conn
	log.Printf("Created persistent connection to %s", addr)
	return conn, nil
}

func (cm *ConnectionManager) markUnhealthy(addr string) {
	cm.mutex.RLock()
	conn, exists := cm.connections[addr]
	cm.mutex.RUnlock()

	if exists {
		conn.mutex.Lock()
		conn.isHealthy = false
		if conn.conn != nil {
			conn.conn.Close()
		}
		conn.mutex.Unlock()
	}
}

func (cm *ConnectionManager) closeAll() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	for _, conn := range cm.connections {
		if conn.conn != nil {
			conn.conn.Close()
		}
	}
	cm.connections = make(map[string]*Connection)
}

// Cleanup old unhealthy connections
func (cm *ConnectionManager) cleanup() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	for addr, conn := range cm.connections {
		conn.mutex.RLock()
		isOld := time.Since(conn.lastUsed) > 5*time.Minute
		isUnhealthy := !conn.isHealthy
		conn.mutex.RUnlock()

		if isOld || isUnhealthy {
			if conn.conn != nil {
				conn.conn.Close()
			}
			delete(cm.connections, addr)
			log.Printf("Cleaned up connection to %s", addr)
		}
	}
}

type Server struct {
	conn               net.Listener
	shuttingDown       bool
	services           []string
	nodeID             int
	totalNodes         int
	isLeader           bool
	peers              []string
	electionInProgress bool
	leaderLastSeen     time.Time
	mutex              sync.RWMutex
	electionMutex      sync.Mutex
	shutdownChan       chan struct{}

	// Connection management
	connManager *ConnectionManager
}

func NewServer(nodeID int, totalNodes int) *Server {
	// Lista de todos los servicios que necesitan monitoreo
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
	}

	// Generate peer addresses (assuming resuscitator nodes are named resuscitator_1, resuscitator_2, etc.)
	peers := make([]string, 0, totalNodes-1)
	for i := 1; i <= totalNodes; i++ {
		if i != nodeID {
			peers = append(peers, fmt.Sprintf("resuscitator_%d:8000", i))
		}
	}

	return &Server{
		services:           services,
		nodeID:             nodeID,
		totalNodes:         totalNodes,
		isLeader:           false,
		peers:              peers,
		electionInProgress: false,
		leaderLastSeen:     time.Now(),
		shutdownChan:       make(chan struct{}),
		connManager:        NewConnectionManager(),
	}
}

func (s *Server) Start() {
	// Start connection cleanup routine
	go s.connectionCleanupRoutine()

	// Start listening for peer communications
	go s.startPeerListener()

	// Wait for services to be ready
	log.Printf("Node %d: Waiting for services to be ready...", s.nodeID)
	time.Sleep(5 * time.Second)

	// Start election process
	go s.startElection()

	// Start leader monitoring (for non-leaders)
	go s.monitorLeader()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.shutdown()
	}()

	log.Printf("Node %d: Resuscitator started", s.nodeID)

	// Simple main loop that waits for shutdown or checks leadership
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var serviceMonitorCancel chan struct{}

	for {
		select {
		case <-s.shutdownChan:
			log.Printf("Node %d: Shutting down...", s.nodeID)
			if serviceMonitorCancel != nil {
				close(serviceMonitorCancel)
			}
			return
		case <-ticker.C:
			s.mutex.RLock()
			isLeader := s.isLeader
			s.mutex.RUnlock()

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

func (s *Server) connectionCleanupRoutine() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			s.connManager.cleanup()
		}
	}
}

func (s *Server) startPeerListener() {
	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Printf("Node %d: Failed to start peer listener: %v", s.nodeID, err)
		return
	}
	defer listener.Close()

	log.Printf("Node %d: Peer listener started on port 8000", s.nodeID)

	for {
		select {
		case <-s.shutdownChan:
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-s.shutdownChan:
					return
				default:
					log.Printf("Node %d: Failed to accept peer connection: %v", s.nodeID, err)
					continue
				}
			}
			go s.handlePeerConnection(conn)
		}
	}
}

func (s *Server) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	// Read the message as a simple string
	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Node %d: Failed to read message: %v", s.nodeID, err)
		return
	}

	message = strings.TrimSpace(message)

	// Parse message format: TYPE_ID
	parts := strings.Split(message, "_")
	if len(parts) != 2 {
		log.Printf("Node %d: Invalid message format: %s", s.nodeID, message)
		return
	}

	msgType := parts[0]
	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Printf("Node %d: Invalid node ID in message: %s", s.nodeID, message)
		return
	}

	log.Printf("Node %d: Received %s message from node %d", s.nodeID, msgType, nodeID)

	switch msgType {
	case ELECTION:
		s.handleElectionMessage(conn, nodeID)
	case OK:
		s.handleOKMessage(nodeID)
	case COORDINATOR:
		s.handleCoordinatorMessage(nodeID)
	case HEARTBEAT:
		s.handleHeartbeatMessage(nodeID)
	}
}

func (s *Server) handleElectionMessage(conn net.Conn, senderNodeID int) {
	// Always respond with OK if we have higher ID
	if s.nodeID > senderNodeID {
		response := fmt.Sprintf("%s_%d\n", OK, s.nodeID)
		conn.Write([]byte(response))

		// Start our own election if not already in progress
		go s.startElection()
	}
}

func (s *Server) handleOKMessage(senderNodeID int) {
	// Someone with higher ID responded, so we wait for coordinator message
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
		go s.sendHeartbeats()
	} else if !s.isLeader && wasLeader {
		log.Printf("Node %d: Node %d is the new leader, I lost leadership", s.nodeID, leaderNodeID)
	} else if !s.isLeader {
		log.Printf("Node %d: Node %d is the new leader", s.nodeID, leaderNodeID)
	}
}

func (s *Server) handleHeartbeatMessage(leaderNodeID int) {
	s.mutex.Lock()
	s.leaderLastSeen = time.Now()
	s.mutex.Unlock()
}

func (s *Server) startElection() {
	s.electionMutex.Lock()
	defer s.electionMutex.Unlock()

	if s.electionInProgress {
		return
	}

	s.electionInProgress = true
	log.Printf("Node %d: Starting election", s.nodeID)

	// First, check if there's already a leader by listening for heartbeats
	log.Printf("Node %d: Checking for existing leader before declaring myself...", s.nodeID)
	time.Sleep(3 * time.Second) // Wait for potential heartbeats

	s.mutex.RLock()
	recentHeartbeat := time.Since(s.leaderLastSeen) < 8*time.Second
	s.mutex.RUnlock()

	if recentHeartbeat {
		log.Printf("Node %d: Detected existing leader, aborting election", s.nodeID)
		s.electionInProgress = false
		return
	}

	// Send ELECTION message to all nodes with higher IDs
	higherNodes := make([]string, 0)
	for i := s.nodeID + 1; i <= s.totalNodes; i++ {
		higherNodes = append(higherNodes, fmt.Sprintf("resuscitator_%d:8000", i))
	}

	if len(higherNodes) == 0 {
		// No higher nodes, I am the leader
		// should change if replicas can resurrect
		s.becomeLeader()
		return
	}

	okReceived := false
	var wg sync.WaitGroup

	for _, peer := range higherNodes {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			if s.sendElectionMessage(peerAddr) {
				okReceived = true
			}
		}(peer)
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
				// No coordinator message received, restart election
				s.electionInProgress = false
				s.mutex.Unlock()
				go s.startElection()
			} else {
				s.mutex.Unlock()
			}
		}()
	}
}

func (s *Server) sendElectionMessage(peerAddr string) bool {
	conn, err := net.DialTimeout("tcp", peerAddr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	message := fmt.Sprintf("%s_%d\n", ELECTION, s.nodeID)
	if _, err := conn.Write([]byte(message)); err != nil {
		return false
	}

	// Wait for OK response
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	response = strings.TrimSpace(response)
	parts := strings.Split(response, "_")
	if len(parts) != 2 {
		return false
	}

	return parts[0] == OK
}

func (s *Server) becomeLeader() {
	s.mutex.Lock()
	s.isLeader = true
	s.electionInProgress = false
	s.mutex.Unlock()

	log.Printf("Node %d: I am the new leader!", s.nodeID)

	// Send COORDINATOR message to all lower nodes
	for i := 1; i < s.nodeID; i++ {
		go s.sendCoordinatorMessage(fmt.Sprintf("resuscitator_%d:8000", i))
	}

	// Start sending heartbeats
	go s.sendHeartbeats()
}

func (s *Server) sendCoordinatorMessage(peerAddr string) {
	conn, err := net.DialTimeout("tcp", peerAddr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	message := fmt.Sprintf("%s_%d\n", COORDINATOR, s.nodeID)
	conn.Write([]byte(message))
}

func (s *Server) sendHeartbeats() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			s.mutex.RLock()
			isLeader := s.isLeader
			s.mutex.RUnlock()

			if !isLeader {
				return
			}

			// Send heartbeat to all nodes using persistent connections
			for _, peer := range s.peers {
				go s.sendHeartbeat(peer)
			}
		}
	}
}

func (s *Server) sendHeartbeat(peerAddr string) {
	conn, err := s.connManager.getConnection(peerAddr)
	if err != nil {
		log.Printf("Node %d: Failed to get connection to %s: %v", s.nodeID, peerAddr, err)
		return
	}

	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	message := fmt.Sprintf("%s_%d\n", HEARTBEAT, s.nodeID)
	conn.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

	if _, err := conn.conn.Write([]byte(message)); err != nil {
		log.Printf("Node %d: Failed to send heartbeat to %s: %v", s.nodeID, peerAddr, err)
		s.connManager.markUnhealthy(peerAddr)
		return
	}

	log.Printf("Node %d: Sent heartbeat to %s via persistent connection", s.nodeID, peerAddr)
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

	// Close all persistent connections
	s.connManager.closeAll()

	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
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

	ticker := time.NewTicker(5 * time.Second)
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

			// Perform health check using persistent connection
			if !s.performHealthCheck(serviceName) {
				s.resurrectService(serviceName)
				// Wait longer after resurrection attempt
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func (s *Server) performHealthCheck(serviceName string) bool {
	serviceAddr := fmt.Sprintf("%s:7000", serviceName)
	conn, err := s.connManager.getConnection(serviceAddr)
	if err != nil {
		log.Printf("Node %d: Service %s health check failed (connection): %v", s.nodeID, serviceName, err)
		return false
	}

	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	// Set timeouts for the health check
	conn.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	conn.conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	if _, err := conn.conn.Write([]byte("PING\n")); err != nil {
		log.Printf("Node %d: Failed to send health check to service %s: %v", s.nodeID, serviceName, err)
		s.connManager.markUnhealthy(serviceAddr)
		return false
	}

	buffer := make([]byte, 1024)
	_, err = conn.conn.Read(buffer)
	if err != nil {
		log.Printf("Node %d: Service %s health check response failed: %v", s.nodeID, serviceName, err)
		s.connManager.markUnhealthy(serviceAddr)
		return false
	}

	log.Printf("Node %d: Service %s is healthy (persistent connection)", s.nodeID, serviceName)
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

	// Mark the service connection as unhealthy so it gets recreated
	serviceAddr := fmt.Sprintf("%s:7000", serviceName)
	s.connManager.markUnhealthy(serviceAddr)

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
