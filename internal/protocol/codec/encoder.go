package codec

import (
	"bytes"
	"encoding/binary"
)

type Encoder struct {
	buff *bytes.Buffer
	err error
}

func NewEncoder() *Encoder {
	return &Encoder{
		buff: bytes.NewBuffer(make([]byte, 0)),
	}
}

func (e *Encoder) WriteUint32(v uint32) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.buff, binary.BigEndian, v)
}

func (e *Encoder) WriteInt32(v int32) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.buff, binary.BigEndian, v)
}

func (e *Encoder) WriteBytes(v []byte) {
	if e.err != nil {
		return
	}
	_, e.err = e.buff.Write(v)
}
