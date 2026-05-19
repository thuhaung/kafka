package codec

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
)

type Request struct {
	ApiKey        int8
	CorrelationID int32
	Body          []byte
}

type Response struct {
	CorrelationID int32
	Body          []byte
}

type Handler interface {
	HandleRequest(ctx context.Context, request *Request) (*Response, error)
}

type Codec interface {
	ReadRequest(conn net.Conn) (*Request, error)
	WriteRequest(conn net.Conn, request *Request) error

	ReadResponse(conn net.Conn) (*Response, error)
	WriteResponse(conn net.Conn, response *Response) error
}

type KafkaCodec struct{}

const (
	correlationIDSize = 4
	apiKeySize = 1
	
	requestHeaderSize = apiKeySize + correlationIDSize
	responseHeaderSize = correlationIDSize
)

var (
	ErrInvalidRequest = errors.New("Invalid request")
	ErrUnsupportedApiKey = errors.New("Unsupported API key")
)

func (kc *KafkaCodec) ReadRequest(conn net.Conn) (*Request, error) {
	buff := make([]byte, 4)

	_, err := io.ReadFull(conn, buff)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(buff)
	if length < requestHeaderSize {
		return nil, ErrInvalidRequest
	}

	data := make([]byte, length)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	request := &Request{
		ApiKey:        int8(data[0]),
		CorrelationID: int32(binary.BigEndian.Uint32(data[1:5])),
		Body:          data[5:],
	}
	
	if err := validateRequest(request); err != nil {
		return nil, err
	}

	return request, nil
}

func (kc *KafkaCodec) WriteRequest(conn net.Conn, request *Request) error {
	if err := validateRequest(request); err != nil {
		return err
	}

	body := request.Body
	payloadLength := apiKeySize + correlationIDSize + uint32(len(body))

	encoder := NewEncoder()
	encoder.WriteUint32(payloadLength)
	encoder.WriteBytes([]byte{byte(request.ApiKey)})
	encoder.WriteInt32(request.CorrelationID)
	encoder.WriteBytes(body)

	if encoder.err != nil {
		return encoder.err
	}
	
	err := writeFull(conn, encoder.buff.Bytes())
	return err
}

func (kc *KafkaCodec) ReadResponse(conn net.Conn) (*Response, error) {
	buff := make([]byte, 4)

	_, err := io.ReadFull(conn, buff)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(buff)
	if length < responseHeaderSize {
		return nil, ErrInvalidRequest
	}

	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	response := &Response{
		CorrelationID: int32(binary.BigEndian.Uint32(data[0:4])),
		Body: data[4:],
	}

	if err := validateResponse(response); err != nil {
		return nil, err
	}

	return response, nil
}

func (kc *KafkaCodec) WriteResponse(conn net.Conn, response *Response) error {
	if err := validateResponse(response); err != nil {
		return err
	}
	
	body := response.Body
	payloadLength := correlationIDSize + uint32(len(body))

	encoder := NewEncoder()
	encoder.WriteUint32(payloadLength)
	encoder.WriteInt32(response.CorrelationID)
	encoder.WriteBytes(body)

	if encoder.err != nil {
		return encoder.err
	}

	err := writeFull(conn, encoder.buff.Bytes())
	return err
}
