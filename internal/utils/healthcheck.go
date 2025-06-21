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
	id          int
	conn        *net.UDPConn
	shutdown    chan struct{}
}

func NewHealthCheckServer(id int, serviceType string) *HealthCheckServer {
	return &HealthCheckServer{
		serviceType: serviceType,
		id:          id,
		shutdown:    make(chan struct{}),
	}

}

func (h *HealthCheckServer) Start() {
	healthCheckAddr := fmt.Sprintf(":%d", 7000)

	addr, err := net.ResolveUDPAddr("udp", healthCheckAddr)
	if err != nil {
		log.Printf("Failed to resolve UDP address %s: %v", healthCheckAddr, err)
		return
	}

	h.conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("Failed to start UDP health check server on %s: %v", healthCheckAddr, err)
		return
	}

	log.Printf("UDP health check server started on port 7000")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		h.stop()
	}()

	h.handleUDPMessages()
}

func (h *HealthCheckServer) handleUDPMessages() {
	buffer := make([]byte, 1024)

	for {
		select {
		case <-h.shutdown:
			return
		default:
			n, addr, err := h.conn.ReadFromUDP(buffer)
			if err != nil {
				select {
				case <-h.shutdown:
					return
				default:
					log.Printf("Error reading UDP health check request: %v", err)
					continue
				}
			}

			message := string(buffer[:n])

			if message == "PING" {
				_, err = h.conn.WriteToUDP([]byte("PONG"), addr)
				if err != nil {
					log.Printf("Error writing UDP health check response: %v", err)
					continue
				}
				log.Printf("Responded to UDP health check ping from %s", addr.String())
			} else {
				log.Printf("Unknown UDP health check message from %s: %s", addr.String(), message)
			}
		}
	}
}

func (h *HealthCheckServer) stop() {
	close(h.shutdown)
	if h.conn != nil {
		h.conn.Close()
	}
	log.Printf("UDP health check server stopped")
}
