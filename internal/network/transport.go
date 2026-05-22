package network

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/thuhaung/kafka/internal/config"
)

type RequestHandler interface {
	HandleRequest(ctx context.Context, request *Request) (*Response, error)
}

type Transport interface {
	ReadRequest(conn net.Conn) (*Request, error)
	WriteRequest(conn net.Conn, request *Request) error

	ReadResponse(conn net.Conn) (*Response, error)
	WriteResponse(conn net.Conn, response *Response) error
}

type KafkaTransport struct{}

var (
	ErrInvalidRequest    = errors.New("Invalid request")
	ErrInvalidResponse   = errors.New("Invalid response")
	ErrUnsupportedApiKey = errors.New("Unsupported API key")
)

func (kt *KafkaTransport) ReadRequest(conn net.Conn) (*Request, error) {
	if err := conn.SetReadDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return nil, err
	}

	buff := make([]byte, 4)

	_, err := io.ReadFull(conn, buff)
	if err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	length := binary.BigEndian.Uint32(buff)
	if length < requestHeaderSize {
		return nil, ErrInvalidRequest
	}

	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	request := &Request{}
	if err := request.Decode(data); err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	return request, nil
}

func (kt *KafkaTransport) WriteRequest(conn net.Conn, request *Request) error {
	if err := conn.SetWriteDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return err
	}

	body, err := request.Encode()
	if err != nil {
		return err
	}

	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(len(body)))

	if err := writeFull(conn, prefix); err != nil {
		return resolveErrorIfTimeout(err)
	}

	err = writeFull(conn, body)
	return resolveErrorIfTimeout(err)
}

func (kt *KafkaTransport) ReadResponse(conn net.Conn) (*Response, error) {
	if err := conn.SetReadDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return nil, err
	}

	buff := make([]byte, 4)

	_, err := io.ReadFull(conn, buff)
	if err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	length := binary.BigEndian.Uint32(buff)
	if length < responseHeaderSize {
		return nil, ErrInvalidResponse
	}

	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	response := &Response{}
	if err := response.Decode(data); err != nil {
		return nil, resolveErrorIfTimeout(err)
	}

	return response, nil
}

func (kt *KafkaTransport) WriteResponse(conn net.Conn, response *Response) error {
	if err := conn.SetWriteDeadline(time.Now().Add(config.API_TIMEOUT)); err != nil {
		return err
	}

	body, err := response.Encode()
	if err != nil {
		return err
	}

	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(len(body)))
	if err := writeFull(conn, prefix); err != nil {
		return resolveErrorIfTimeout(err)
	}

	err = writeFull(conn, body)
	return resolveErrorIfTimeout(err)
}
