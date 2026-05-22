package network

import (
	"errors"

	"log"
	"net"

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

func (client *Client) Send(request *Request) (*Response, error) {
	conn, err := net.DialTimeout("tcp", ":" + client.dest, config.API_TIMEOUT)
	if err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	defer conn.Close()

	log.Println("Sending request to:", client.dest)

	err = client.transport.WriteRequest(conn, request)
	if err != nil {
		return nil, err
	}

	response, err := client.transport.ReadResponse(conn)
	if err != nil {
		return nil, err
	}

	return response, nil
}
