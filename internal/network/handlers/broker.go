package handlers

import (
	"context"
	"fmt"

	"github.com/thuhaung/kafka/internal/network"
)

type BrokerHandler struct{}

func (handler *BrokerHandler) HandleRequest(ctx context.Context, request *network.Request) (*network.Response, error) {
	switch request.ApiKey {
		default:
			return &network.Response{
				CorrelationID: request.CorrelationID,
			}, fmt.Errorf("%w: %d", network.ErrUnsupportedApiKey, request.ApiKey)
	}
}
