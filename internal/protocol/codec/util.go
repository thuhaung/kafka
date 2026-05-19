package codec

import "io"

func validateRequest(request *Request) error {
	if request.CorrelationID <= 0 {
		return ErrInvalidRequest
	}
	return nil
}

func validateResponse(response *Response) error {
	if response.CorrelationID <= 0 {
		return ErrInvalidRequest
	}
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
