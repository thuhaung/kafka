package client

import (
	"net"
	"github.com/thuhaung/kafka/internal/protocol/codec"
)

type Client struct {
	dest string
	codec codec.Codec
}

func NewClient(dest string, codec codec.Codec) (*Client, error) {
	return &Client{
		dest,
		codec,
	}, nil
}

func (client *Client) Send(request *codec.Request) (*codec.Response, error) {
	conn, err := net.Dial("tcp", ":" + client.dest)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	if err := client.codec.WriteRequest(conn, request); err != nil {
		return nil, err
	}

	response, err := client.codec.ReadResponse(conn)
	if err != nil {
		return nil, err
	}

	return response, nil
}
