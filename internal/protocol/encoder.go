package protocol

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

func (e *Encoder) GetError() error {
	return e.err
}

func (e *Encoder) GetBytes() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.buff.Bytes(), nil
}

func (e *Encoder) write(v any) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.buff, binary.BigEndian, v)
}

func (e *Encoder) WriteInt8(v int8) {
	if e.err != nil {
		return
	}
	e.err = e.buff.WriteByte(byte(v))
}

func (e *Encoder) WriteUint8(v uint8) {
	if e.err != nil {
		return
	}
	e.err = e.buff.WriteByte(v)
}

func (e *Encoder) WriteInt16(v int16) {
	e.write(v)
}

func (e *Encoder) WriteUint16(v uint16) {
	e.write(v)
}

func (e *Encoder) WriteUint32(v uint32) {
	e.write(v)
}

func (e *Encoder) WriteInt32(v int32) {
	e.write(v)
}

func (e *Encoder) WriteUint64(v uint64) {
	e.write(v)
}

func (e *Encoder) WriteInt64(v int64) {
	e.write(v)
}

func (e *Encoder) WriteBool(v bool) {
	if v {
		e.WriteUint8(1)
		return
	}
	e.WriteUint8(0)
}

func (e *Encoder) WriteBytes(v []byte) {
	if e.err != nil {
		return
	}
	_, e.err = e.buff.Write(v)
}

func (e *Encoder) WriteString(v string) {
	e.WriteBytes([]byte(v))
}

func (e *Encoder) WriteBytesWithUint32Length(v []byte) {
	e.WriteUint32(uint32(len(v)))
	e.WriteBytes(v)
}

func (e *Encoder) WriteStringWithUint16Length(v string) {
	e.WriteUint16(uint16(len(v)))
	e.WriteString(v)
}
