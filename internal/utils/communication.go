package utils

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func MessageFromSocket(socket *net.Conn) ([]byte, error) {
	reader := bufio.NewReader(*socket)
	u8Buffer := make([]byte, 4)
	_, err := io.ReadFull(reader, u8Buffer)
	if err != nil {
		return nil, err
	}

	messageLength := binary.BigEndian.Uint32(u8Buffer)
	payload := make([]byte, messageLength)
	_, err = io.ReadFull(reader, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func SendMessage(conn net.Conn, message []byte) error {
	messageLength := len(message)
	completeMessage := make([]byte, 4+messageLength)

	binary.BigEndian.PutUint32(completeMessage[:4], uint32(messageLength))

	copy(completeMessage[4:], message)

	totalWritten := 0
	for totalWritten < len(completeMessage) {
		n, err := conn.Write(completeMessage[totalWritten:])
		if err != nil {
			return fmt.Errorf("Error writing to connection: %v", err)
		}

		if n == 0 {
			return fmt.Errorf("Connection closed while writing")
		}

		totalWritten += n
	}

	return nil
}
