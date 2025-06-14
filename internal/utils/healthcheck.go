package utils

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

)

type HealthCheckServer struct {
	serviceType string
	id       int
	listener net.Listener
	shutdown chan struct{}
}

func NewHealthCheckServer(id int, serviceType string) *HealthCheckServer {
	return &HealthCheckServer{
		serviceType: serviceType,
		id:       id,
		shutdown: make(chan struct{}),
	}

}

func (h *HealthCheckServer) Start() {
	healthCheckPort := fmt.Sprintf("%s:7000", h.serviceType)

	listener, err := net.Listen("tcp", healthCheckPort)
	if err != nil {
		log.Printf("Failed to start health check server on port %s: %v", healthCheckPort, err)
		return
	}
	h.listener = listener

	log.Printf("Health check server started on port %s", healthCheckPort)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		h.stop()
	}()

	for {
		select {
		case <-h.shutdown:
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-h.shutdown:
					return
				default:
					log.Printf("Error accepting health check connection: %v", err)
					continue
				}
			}

			go h.handleConnection(conn)
		}
	}
}

func (h *HealthCheckServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Printf("Error reading health check request: %v", err)
		return
	}

	message := string(buffer[:n])

	if message == "PING\n" || message == "PING" {
		_, err = conn.Write([]byte("PONG\n"))
		if err != nil {
			log.Printf("Error writing health check response: %v", err)
			return
		}
		log.Printf("Responded to health check ping")
	} else {
		log.Printf("Unknown health check message: %s", message)
	}
}

func (h *HealthCheckServer) stop() {
	close(h.shutdown)
	if h.listener != nil {
		h.listener.Close()
	}
	log.Printf("Health check server stopped")
}
