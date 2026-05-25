package network

import "github.com/thuhaung/kafka/internal/protocol"

type Request struct {
	ApiKey        int8
	CorrelationID int32
	Body          []byte
}

type Response struct {
	CorrelationID int32
	Body          []byte
}

const (
	correlationIDSize = 4
	apiKeySize        = 1

	requestHeaderSize  = apiKeySize + correlationIDSize
	responseHeaderSize = correlationIDSize
)

func (response *Response) Encode() ([]byte, error) {
	if err := validateResponse(*response); err != nil {
		return nil, err
	}

	encoder := protocol.NewEncoder()
	encoder.WriteInt32(response.CorrelationID)
	encoder.WriteBytes(response.Body)

	if err := encoder.GetError(); err != nil {
		return nil, err
	}

	return encoder.GetBytes()
}

func (response *Response) Decode(data []byte) error {
	decoder := protocol.NewDecoder(data)

	*response = Response{
		CorrelationID: decoder.ReadInt32(),
		Body:          decoder.ReadBytes(decoder.Remaining()),
	}
	if decoder.GetError() != nil {
		return decoder.GetError()
	}
	if err := validateResponse(*response); err != nil {
		return err
	}

	return nil
}

func (request *Request) Encode() ([]byte, error) {
	if err := validateRequest(*request); err != nil {
		return nil, err
	}

	encoder := protocol.NewEncoder()
	encoder.WriteInt8(request.ApiKey)
	encoder.WriteInt32(request.CorrelationID)
	encoder.WriteBytes(request.Body)

	if err := encoder.GetError(); err != nil {
		return nil, err
	}

	return encoder.GetBytes()
}

func (request *Request) Decode(data []byte) error {
	decoder := protocol.NewDecoder(data)

	*request = Request{
		ApiKey:        decoder.ReadInt8(),
		CorrelationID: decoder.ReadInt32(),
		Body:          decoder.ReadBytes(decoder.Remaining()),
	}
	if decoder.GetError() != nil {
		return decoder.GetError()
	}
	if err := validateRequest(*request); err != nil {
		return err
	}

	return nil
}

type ApiKey int

const (
	RegisterBrokerAndFetchMetadataAPIKey ApiKey = 1
	FetchMetadataImageAPIKey             ApiKey = 2
)
