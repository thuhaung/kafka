package client

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/thuhaung/kafka/internal/config"
	"github.com/thuhaung/kafka/internal/protocol/codec"
)

type Client struct {
	dest string
	codec codec.Codec
}

var (
	ErrRequestTimeout = errors.New("Request timed out")
)

func NewClient(dest string, codec codec.Codec) (*Client, error) {
	return &Client{
		dest,
		codec,
	}, nil
}

func isTimeout(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded)
}

func (client *Client) Send(request *codec.Request) (*codec.Response, error) {
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

	err = client.codec.WriteRequest(conn, request)
	if err != nil {
		if isTimeout(err) {
			return nil, ErrRequestTimeout
		}
		return nil, fmt.Errorf("Error writing request: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return nil, fmt.Errorf("Error setting read deadline: %w", err)
	}

	response, err := client.codec.ReadResponse(conn)
	if err != nil {
		if isTimeout(err) {
			return nil, ErrRequestTimeout
		}
		return nil, fmt.Errorf("Error reading response: %w", err)
	}

	return response, nil
}
