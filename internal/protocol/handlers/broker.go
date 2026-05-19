package handlers

import (
	"context"
	"fmt"
	"github.com/thuhaung/kafka/internal/protocol/codec"
)

type BrokerHandler struct{}

func (handler *BrokerHandler) HandleRequest(ctx context.Context, request *codec.Request) (*codec.Response, error) {
	switch request.ApiKey {
		default:
			return &codec.Response{
				CorrelationID: request.CorrelationID,
			}, fmt.Errorf("%w: %d", codec.ErrUnsupportedApiKey, request.ApiKey)
	}
}
