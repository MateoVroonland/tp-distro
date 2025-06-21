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

// PeerConnection holds a persistent bidirectional connection with a peer
type PeerConnection struct {
	conn       net.Conn
	peerID     int
	addr       string
	isHealthy  bool
	writeMutex sync.Mutex
	readMutex  sync.Mutex
}

// Connection holds a persistent connection with metadata (for services)
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
	electionInProgress bool
	leaderLastSeen     time.Time
	mutex              sync.RWMutex
	electionMutex      sync.Mutex
	shutdownChan       chan struct{}

	// Connection management
	connManager   *ConnectionManager      // For service health checks
	peerConns     map[int]*PeerConnection // For peer-to-peer communication
	peerConnMutex sync.RWMutex

	// Heartbeat management
	heartbeatCancel chan struct{}
	heartbeatMutex  sync.Mutex

	// Leadership change notifications
	leadershipChangeChan chan bool
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

	return &Server{
		services:             services,
		nodeID:               nodeID,
		totalNodes:           totalNodes,
		isLeader:             false,
		electionInProgress:   false,
		leaderLastSeen:       time.Now(),
		shutdownChan:         make(chan struct{}),
		connManager:          NewConnectionManager(),
		peerConns:            make(map[int]*PeerConnection),
		peerConnMutex:        sync.RWMutex{},
		heartbeatCancel:      nil,
		heartbeatMutex:       sync.Mutex{},
		leadershipChangeChan: make(chan bool, 1), // Buffered channel to avoid blocking
	}
}

func (s *Server) Start() {
	// Start connection cleanup routine
	go s.connectionCleanupRoutine()

	// Wait for services to be ready
	log.Printf("Node %d: Waiting for services to be ready...", s.nodeID)
	time.Sleep(5 * time.Second)

	// Establish persistent connections with all peers
	s.establishPeerConnections()

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

	// Main loop that waits for shutdown or leadership changes
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

func (s *Server) establishPeerConnections() {
	log.Printf("Node %d: Establishing persistent connections with all peers...", s.nodeID)

	// We only establish connections to peers with higher IDs to avoid duplicates
	// Each pair of nodes will have exactly one bidirectional connection
	for i := s.nodeID + 1; i <= s.totalNodes; i++ {
		peerAddr := fmt.Sprintf("resuscitator_%d:8000", i)
		go s.connectToPeer(i, peerAddr)
	}

	// Also listen for incoming connections from peers with lower IDs
	go s.startPeerListener()

	// Give connections time to establish
	time.Sleep(3 * time.Second)
	log.Printf("Node %d: Peer connections established", s.nodeID)
}

func (s *Server) connectToPeer(peerID int, addr string) {
	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			log.Printf("Node %d: Failed to connect to peer %d (attempt %d/%d): %v", s.nodeID, peerID, attempt, maxRetries, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// Send HELLO message to identify ourselves
		helloMsg := fmt.Sprintf("HELLO_%d\n", s.nodeID)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(helloMsg)); err != nil {
			log.Printf("Node %d: Failed to send HELLO to peer %d: %v", s.nodeID, peerID, err)
			conn.Close()
			continue
		}

		// Wait for HELLO_ACK response
		reader := bufio.NewReader(conn)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		response, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Node %d: Failed to read HELLO_ACK from peer %d: %v", s.nodeID, peerID, err)
			conn.Close()
			continue
		}

		response = strings.TrimSpace(response)
		expectedAck := fmt.Sprintf("HELLO_ACK_%d", peerID)
		if response != expectedAck {
			log.Printf("Node %d: Invalid HELLO_ACK from peer %d: %s", s.nodeID, peerID, response)
			conn.Close()
			continue
		}

		// Enable TCP keep-alive
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		peerConn := &PeerConnection{
			conn:      conn,
			peerID:    peerID,
			addr:      addr,
			isHealthy: true,
		}

		s.peerConnMutex.Lock()
		s.peerConns[peerID] = peerConn
		s.peerConnMutex.Unlock()

		log.Printf("Node %d: Established connection to peer %d at %s", s.nodeID, peerID, addr)

		// Start reading from this connection
		go s.handlePeerMessages(peerConn)
		return
	}

	log.Printf("Node %d: Failed to connect to peer %d after %d attempts", s.nodeID, peerID, maxRetries)
}

func (s *Server) handlePeerMessages(peerConn *PeerConnection) {
	defer func() {
		peerConn.conn.Close()
		s.peerConnMutex.Lock()
		peerConn.isHealthy = false
		s.peerConnMutex.Unlock()
		log.Printf("Node %d: Connection to peer %d closed", s.nodeID, peerConn.peerID)
	}()

	reader := bufio.NewReader(peerConn.conn)

	for {
		select {
		case <-s.shutdownChan:
			return
		default:
			// Set read timeout - shorter timeout for faster failure detection
			peerConn.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

			message, err := reader.ReadString('\n')
			if err != nil {
				if err.Error() == "EOF" {
					log.Printf("Node %d: Peer %d disconnected", s.nodeID, peerConn.peerID)
				} else {
					log.Printf("Node %d: Failed to read from peer %d: %v", s.nodeID, peerConn.peerID, err)
				}
				return
			}

			message = strings.TrimSpace(message)
			s.processPeerMessage(message, peerConn)
		}
	}
}

func (s *Server) processPeerMessage(message string, peerConn *PeerConnection) {
	// Parse message format: TYPE_ID
	parts := strings.Split(message, "_")
	if len(parts) != 2 {
		log.Printf("Node %d: Invalid message format from peer %d: %s", s.nodeID, peerConn.peerID, message)
		return
	}

	msgType := parts[0]
	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Printf("Node %d: Invalid node ID in message from peer %d: %s", s.nodeID, peerConn.peerID, message)
		return
	}

	log.Printf("Node %d: Received %s message from node %d", s.nodeID, msgType, nodeID)

	switch msgType {
	case ELECTION:
		s.handleElectionMessagePersistent(peerConn, nodeID)
	case OK:
		s.handleOKMessage(nodeID)
	case COORDINATOR:
		s.handleCoordinatorMessage(nodeID)
	case HEARTBEAT:
		s.handleHeartbeatMessage(nodeID)
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
			go s.handleIncomingPeerConnection(conn)
		}
	}
}


func (s *Server) handleIncomingPeerConnection(conn net.Conn) {
	defer conn.Close()

	// First, read a HELLO message to identify the peer
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	helloMsg, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Node %d: Failed to read HELLO from incoming peer: %v", s.nodeID, err)
		return
	}

	helloMsg = strings.TrimSpace(helloMsg)
	if !strings.HasPrefix(helloMsg, "HELLO_") {
		log.Printf("Node %d: Invalid HELLO message from incoming peer: %s", s.nodeID, helloMsg)
		return
	}

	peerIDStr := strings.TrimPrefix(helloMsg, "HELLO_")
	peerID, err := strconv.Atoi(peerIDStr)
	if err != nil {
		log.Printf("Node %d: Invalid peer ID in HELLO: %s", s.nodeID, peerIDStr)
		return
	}

	// Only accept connections from peers with lower IDs
	if peerID >= s.nodeID {
		log.Printf("Node %d: Rejecting connection from peer %d (should connect in reverse)", s.nodeID, peerID)
		return
	}

	// Send HELLO response
	response := fmt.Sprintf("HELLO_ACK_%d\n", s.nodeID)
	conn.Write([]byte(response))

	// Enable TCP keep-alive
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Create peer connection and register it
	peerConn := &PeerConnection{
		conn:      conn,
		peerID:    peerID,
		addr:      conn.RemoteAddr().String(),
		isHealthy: true,
	}

	s.peerConnMutex.Lock()
	s.peerConns[peerID] = peerConn
	s.peerConnMutex.Unlock()

	log.Printf("Node %d: Accepted connection from peer %d", s.nodeID, peerID)

	// Start reading messages from this connection
	s.handlePeerMessages(peerConn)
}

func (s *Server) handleElectionMessagePersistent(peerConn *PeerConnection, senderNodeID int) {
	// Always respond with OK if we have higher ID
	if s.nodeID > senderNodeID {
		response := fmt.Sprintf("%s_%d\n", OK, s.nodeID)
		s.sendToPeer(peerConn.peerID, response)

		// Start our own election if not already in progress
		go s.startElection()
	}
}

func (s *Server) sendToPeer(peerID int, message string) error {
	s.peerConnMutex.RLock()
	peerConn, exists := s.peerConns[peerID]
	s.peerConnMutex.RUnlock()

	if !exists || !peerConn.isHealthy {
		return fmt.Errorf("no healthy connection to peer %d", peerID)
	}

	peerConn.writeMutex.Lock()
	defer peerConn.writeMutex.Unlock()

	peerConn.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err := peerConn.conn.Write([]byte(message))
	if err != nil {
		log.Printf("Node %d: Failed to send message to peer %d: %v", s.nodeID, peerID, err)
		peerConn.isHealthy = false
		return err
	}

	return nil
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
		s.startHeartbeats()
		// Notify about leadership change
		select {
		case s.leadershipChangeChan <- true:
		default:
			// Channel is full, skip notification
		}
	} else if !s.isLeader && wasLeader {
		log.Printf("Node %d: Node %d is the new leader, I lost leadership", s.nodeID, leaderNodeID)
		s.stopHeartbeats()
		// Notify about leadership loss
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
			if s.sendElectionMessagePersistent(targetPeerID) {
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


func (s *Server) sendElectionMessagePersistent(peerID int) bool {
	message := fmt.Sprintf("%s_%d\n", ELECTION, s.nodeID)
	err := s.sendToPeer(peerID, message)
	if err != nil {
		log.Printf("Node %d: Failed to send ELECTION to peer %d: %v", s.nodeID, peerID, err)
		return false
	}

	// For persistent connections, we don't wait for OK response here
	// The response will be handled by the message reader goroutine
	// We'll use a simple timeout approach - assume OK will come through normal message flow
	return true
}

func (s *Server) becomeLeader() {
	s.mutex.Lock()
	s.isLeader = true
	s.electionInProgress = false
	s.mutex.Unlock()

	log.Printf("Node %d: I am the new leader!", s.nodeID)

	// Send COORDINATOR message to all lower nodes
	for i := 1; i < s.nodeID; i++ {
		go s.sendCoordinatorMessagePersistent(i)
	}

	// Start sending heartbeats
	s.startHeartbeats()

	// Notify about becoming leader
	select {
	case s.leadershipChangeChan <- true:
	default:
		// Channel is full, skip notification
	}
}

func (s *Server) startHeartbeats() {
	s.heartbeatMutex.Lock()
	defer s.heartbeatMutex.Unlock()

	// Stop any existing heartbeat goroutine
	if s.heartbeatCancel != nil {
		close(s.heartbeatCancel)
	}

	// Create new cancel channel
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

func (s *Server) sendCoordinatorMessagePersistent(peerID int) {
	message := fmt.Sprintf("%s_%d\n", COORDINATOR, s.nodeID)
	err := s.sendToPeer(peerID, message)
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
			// Log every 10th heartbeat (every 30 seconds) to reduce spam
			if heartbeatCount%10 == 0 {
				log.Printf("Node %d: Sending heartbeats (round %d)", s.nodeID, heartbeatCount)
			}

			// Send heartbeat to all peer nodes
			for i := 1; i <= s.totalNodes; i++ {
				if i != s.nodeID {
					go s.sendHeartbeatPersistent(i)
				}
			}
		}
	}
}

func (s *Server) sendHeartbeatPersistent(peerID int) {
	message := fmt.Sprintf("%s_%d\n", HEARTBEAT, s.nodeID)
	err := s.sendToPeer(peerID, message)
	if err != nil {
		// Try to reconnect once
		s.attemptReconnectToPeer(peerID)

		// Try sending again after reconnection attempt
		err = s.sendToPeer(peerID, message)
		if err != nil {
			// Only log final failures occasionally to avoid spam
			if s.nodeID == 3 { // Only log from leader
				log.Printf("Node %d: Failed to send heartbeat to peer %d after reconnect: %v", s.nodeID, peerID, err)
			}
		}
		return
	}
}

func (s *Server) attemptReconnectToPeer(peerID int) {
	// Only attempt reconnection if we should connect to this peer (higher ID)
	if peerID <= s.nodeID {
		return
	}

	s.peerConnMutex.Lock()
	existingConn, exists := s.peerConns[peerID]
	if exists && existingConn != nil {
		// Mark as unhealthy and close existing connection
		existingConn.isHealthy = false
		if existingConn.conn != nil {
			existingConn.conn.Close()
		}
		delete(s.peerConns, peerID)
	}
	s.peerConnMutex.Unlock()

	// Attempt single reconnection
	peerAddr := fmt.Sprintf("resuscitator_%d:8000", peerID)
	log.Printf("Node %d: Attempting to reconnect to peer %d", s.nodeID, peerID)

	go s.connectToPeer(peerID, peerAddr)
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

	// Close all service connections
	s.connManager.closeAll()

	// Close all peer connections
	s.peerConnMutex.Lock()
	for peerID, peerConn := range s.peerConns {
		if peerConn.conn != nil {
			peerConn.conn.Close()
		}
		log.Printf("Node %d: Closed connection to peer %d", s.nodeID, peerID)
	}
	s.peerConns = make(map[int]*PeerConnection)
	s.peerConnMutex.Unlock()

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
