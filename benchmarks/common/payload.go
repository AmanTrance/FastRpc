package common

import (
	"crypto/rand"
	"log"
)

const PayloadSize = 1 * 1024 * 1024

func GeneratePayload() []byte {
	payload := make([]byte, PayloadSize)
	_, err := rand.Read(payload)
	if err != nil {
		log.Fatalf("Failed to generate random payload: %v", err)
	}
	return payload
}
