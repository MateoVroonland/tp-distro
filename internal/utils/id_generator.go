package utils

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
)
const idLength = 16

func GenerateRandomID() string {
	numBytes := (idLength * 3) / 4

	randomBytes := make([]byte, numBytes)
	rand.Read(randomBytes)
	

	base64Encoded := base64.StdEncoding.EncodeToString(randomBytes)

	base64Encoded = strings.ReplaceAll(base64Encoded, "=", "")

	return base64Encoded[:idLength]
}