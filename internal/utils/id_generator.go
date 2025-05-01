package utils

import (
	"crypto/rand"
	"encoding/hex"
)

const idLength = 16

func GenerateRandomID() string {
	numBytes := (idLength * 3) / 4

	randomBytes := make([]byte, numBytes)
	rand.Read(randomBytes)

	hexEncoded := hex.EncodeToString(randomBytes)

	return hexEncoded[:idLength]
}
