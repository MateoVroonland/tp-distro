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
)

type Server struct {
	conn         net.Listener
	shuttingDown bool
	services     []string
}

func NewServer() *Server {
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
		services: services,
	}
}

func (s *Server) Start() {
	// wait for services to be ready
	log.Printf("Waiting for services to be ready...")
	time.Sleep(5 * time.Second)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.shutdown()
	}()

	log.Printf("Starting resuscitator for %d services...", len(s.services))

	s.monitorAllServices()

	log.Printf("Resuscitator started")
}

func (s *Server) shutdown() {
	s.shuttingDown = true

	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func (s *Server) monitorAllServices() {
	log.Printf("Starting monitoring for all services...")
	var wg sync.WaitGroup

	for _, serviceName := range s.services {
		wg.Add(1)
		go func(service string) {
			defer wg.Done()
			s.monitorService(service)
		}(serviceName)
	}

	wg.Wait()
}

func (s *Server) monitorService(serviceName string) {
	log.Printf("Starting monitoring for service: %s", serviceName)

	for {
		if s.shuttingDown {
			log.Printf("Stopping monitoring for service: %s", serviceName)
			return
		}

		healthCheckPort := fmt.Sprintf("%s:7000", serviceName)
		conn, err := net.DialTimeout("tcp", healthCheckPort, 5*time.Second)

		if err != nil {
			log.Printf("Service %s health check failed: %v", serviceName, err)
			s.resurrectService(serviceName)
			time.Sleep(10 * time.Second)
			continue
		}

		_, err = conn.Write([]byte("PING\n"))
		if err != nil {
			log.Printf("Failed to send health check to service %s: %v", serviceName, err)
			conn.Close()
			s.resurrectService(serviceName)
			time.Sleep(10 * time.Second)
			continue
		}

		buffer := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Read(buffer)

		if err != nil {
			log.Printf("Service %s health check response failed: %v", serviceName, err)
			conn.Close()
			s.resurrectService(serviceName)
			time.Sleep(10 * time.Second)
			continue
		}

		conn.Close()
		log.Printf("Service %s is healthy", serviceName)

		// Wait before next health check
		time.Sleep(5 * time.Second)
	}
}

func (s *Server) resurrectService(serviceName string) {
	log.Printf("Attempting to resurrect service: %s", serviceName)

	log.Printf("Restarting Docker service: %s", serviceName)

	if err := s.restartDockerService(serviceName); err != nil {
		log.Printf("Docker-compose restart failed for service %s: %v", serviceName, err)
		return
	}
	log.Printf("Successfully restarted service %s via docker-compose", serviceName)

	log.Printf("Waiting for service %s to become ready...", serviceName)
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
		log.Printf("Failed to restart service %s: %v, output: %s", serviceName, err, string(output))
		return err
	}

	log.Printf("Service %s restart output: %s", serviceName, string(output))
	return nil
}
