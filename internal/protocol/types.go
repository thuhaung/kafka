package protocol

type Encodable interface {
	Encode() ([]byte, error)
}

type Decodable interface {
	Decode(data []byte) error
}