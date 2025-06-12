package resuscitator

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/MateoVroonland/tp-distro/internal/env"
)

type Server struct {
	conn         net.Listener
	shuttingDown bool
	serviceType  string
}

func NewServer(serviceType string) *Server {
	return &Server{
		serviceType: serviceType,
	}
}

func (s *Server) Start() {
	// wait for replicas to be ready

	log.Printf("Waiting for replicas to be ready...")
	time.Sleep(3 * time.Second)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.shutdown()
	}()

	log.Printf("Listening to request handler...")
	conn, err := net.Listen("tcp", fmt.Sprintf("localhost:500%d", env.AppEnv.ID))
	if err != nil {
		log.Fatalf("Failed to listen to request handler: %v", err)
	}
	s.conn = conn

	s.listenToNodes()

	log.Printf("Resuscitator started")
}

func (s *Server) shutdown() {
	s.shuttingDown = true

	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func (s *Server) listenToNodes() {
	log.Printf("Listening to nodes...")
	var wg sync.WaitGroup

	for i := 1; i <= env.AppEnv.REPLICAS; i++ {
		wg.Add(1)
		go func(nodeID int) {
			defer wg.Done()
			s.listenToNode(nodeID)
		}(i)
	}

	wg.Wait()
}

func (s *Server) listenToNode(nodeID int) {
	for {
		if s.shuttingDown {
			return
		}

		healthCheckPort := fmt.Sprintf("%s_%d:7000", env.AppEnv.SERVICE_TYPE, nodeID)
		conn, err := net.DialTimeout("tcp", healthCheckPort, 5*time.Second)

		if err != nil {
			log.Printf("Node %d health check failed: %v", nodeID, err)
			s.resurrect(nodeID)
			time.Sleep(10 * time.Second)
			continue
		}

		_, err = conn.Write([]byte("PING\n"))
		log.Printf("Sent health check to node %d", nodeID)
		if err != nil {
			log.Printf("Failed to send health check to node %d: %v", nodeID, err)
			conn.Close()
			s.resurrect(nodeID)
			time.Sleep(10 * time.Second)
			continue
		}

		buffer := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Read(buffer)
		log.Printf("Received health check response from node %d", nodeID)

		if err != nil {
			log.Printf("Node %d health check response failed: %v", nodeID, err)
			conn.Close()
			s.resurrect(nodeID)
			time.Sleep(10 * time.Second)
			continue
		}

		conn.Close()

		time.Sleep(5 * time.Second)
	}
}

func (s *Server) resurrect(nodeID int) {
	log.Printf("Attempting to resurrect node %d", nodeID)

	serviceName := s.getServiceName(nodeID)
	if serviceName == "" {
		log.Printf("Could not determine service name for node %d", nodeID)
		return
	}

	log.Printf("Restarting Docker service: %s", serviceName)

	if err := s.restartDockerService(serviceName); err != nil {
		log.Printf("Docker-compose restart failed for service %s: %v", serviceName, err)
		return
	}
	log.Printf("Successfully restarted service %s via docker-compose", serviceName)

	log.Printf("Waiting for service %s to become ready...", serviceName)
	time.Sleep(10 * time.Second)
}

func (s *Server) getServiceName(nodeID int) string {
	if s.serviceType == "" {
		log.Printf("No SERVICE_TYPE specified, cannot determine service name for node %d", nodeID)
		return ""
	}

	// Docker service names follow the pattern: servicetype_nodeID
	// NodeID is already 1-based from the calling loop
	return fmt.Sprintf("%s_%d", s.serviceType, nodeID)
}

func (s *Server) restartDockerService(serviceName string) error {
	workDir := "/app"

	// Use docker compose restart to restart only the specific service
	restartCmd := exec.Command("docker", "compose", "restart", serviceName)
	restartCmd.Dir = workDir

	// Capture output for debugging
	output, err := restartCmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to restart service %s: %v, output: %s", serviceName, err, string(output))
		return err
	}

	log.Printf("Service %s restart output: %s", serviceName, string(output))
	return nil
}
