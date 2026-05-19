package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"time"
)

const (
	lengthPrefixSize  = 4
	apiKeySize        = 1
	correlationIDSize = 4
)

func main() {
	addr := flag.String("addr", "localhost:9092", "broker address")
	apiKey := flag.Int("api-key", 1, "request API key")
	correlationID := flag.Int("correlation-id", 123, "request correlation id")
	body := flag.String("body", "hello", "request body")
	timeout := flag.Duration("timeout", 5*time.Second, "connect/read/write timeout")
	flag.Parse()

	conn, err := net.DialTimeout("tcp", *addr, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(*timeout)); err != nil {
		log.Fatal(err)
	}

	requestBody := []byte(*body)
	frameLength := apiKeySize + correlationIDSize + len(requestBody)
	frame := make([]byte, lengthPrefixSize+frameLength)

	binary.BigEndian.PutUint32(frame[:lengthPrefixSize], uint32(frameLength))
	frame[lengthPrefixSize] = byte(*apiKey)
	binary.BigEndian.PutUint32(frame[lengthPrefixSize+apiKeySize:lengthPrefixSize+apiKeySize+correlationIDSize], uint32(*correlationID))
	copy(frame[lengthPrefixSize+apiKeySize+correlationIDSize:], requestBody)

	if _, err := conn.Write(frame); err != nil {
		log.Fatal(err)
	}
}
