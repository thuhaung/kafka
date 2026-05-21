package network

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/thuhaung/kafka/internal/config"
)

type Client struct {
	dest string
	transport Transport
}

var (
	ErrRequestTimeout = errors.New("Request timed out")
)

func NewClient(dest string, transport Transport) (*Client, error) {
	return &Client{
		dest:      dest,
		transport: transport,
	}, nil
}

func isTimeout(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded)
}

func (client *Client) Send(request *Request) (*Response, error) {
	conn, err := net.DialTimeout("tcp", ":" + client.dest, config.API_TIMEOUT)
	if err != nil {
		if isTimeout(err) {
			return nil, ErrRequestTimeout
		}
		return nil, err
	}

	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return nil, fmt.Errorf("Error setting write deadline: %w", err)
	}

	log.Println("Sending request to:", client.dest)

	err = client.transport.WriteRequest(conn, request)
	if err != nil {
		if isTimeout(err) {
			return nil, ErrRequestTimeout
		}
		return nil, fmt.Errorf("Error writing request: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return nil, fmt.Errorf("Error setting read deadline: %w", err)
	}

	response, err := client.transport.ReadResponse(conn)
	if err != nil {
		if isTimeout(err) {
			return nil, ErrRequestTimeout
		}
		return nil, fmt.Errorf("Error reading response: %w", err)
	}

	return response, nil
}
