package network

import (
	"errors"
	"fmt"
	"net"
	"os"
)

func writeFull(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := conn.Write(data[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	return nil
}

func validateRequest(request Request) error {
	if (request.ApiKey <= 0) || (request.CorrelationID <= 0) {
		return ErrInvalidRequest
	}
	return nil
}

func validateResponse(response Response) error {
	if response.CorrelationID <= 0 {
		return ErrInvalidResponse
	}
	return nil
}

func resolveErrorIfTimeout(err error) error {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%w: %d", ErrRequestTimeout, err)
	}
	return err
}
