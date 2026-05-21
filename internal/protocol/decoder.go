package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Decoder struct {
	reader *bytes.Reader
	err    error
}

func NewDecoder(data []byte) *Decoder {
	return &Decoder{
		reader: bytes.NewReader(data),
	}
}

func (d *Decoder) GetError() error {
	return d.err
}

func (d *Decoder) Remaining() int {
	if d.err != nil {
		return 0
	}
	return d.reader.Len()
}

func (d *Decoder) read(v any) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.reader, binary.BigEndian, v)
}

func (d *Decoder) ReadInt8() int8 {
	if d.err != nil {
		return 0
	}

	b, err := d.reader.ReadByte()
	if err != nil {
		d.err = err
		return 0
	}

	return int8(b)
}

func (d *Decoder) ReadUint8() uint8 {
	if d.err != nil {
		return 0
	}

	b, err := d.reader.ReadByte()
	if err != nil {
		d.err = err
		return 0
	}

	return b
}

func (d *Decoder) ReadInt16() int16 {
	var v int16
	d.read(&v)
	return v
}

func (d *Decoder) ReadUint16() uint16 {
	var v uint16
	d.read(&v)
	return v
}

func (d *Decoder) ReadInt32() int32 {
	var v int32
	d.read(&v)
	return v
}

func (d *Decoder) ReadUint32() uint32 {
	var v uint32
	d.read(&v)
	return v
}

func (d *Decoder) ReadInt64() int64 {
	var v int64
	d.read(&v)
	return v
}

func (d *Decoder) ReadUint64() uint64 {
	var v uint64
	d.read(&v)
	return v
}

func (d *Decoder) ReadBool() bool {
	return d.ReadUint8() != 0
}

func (d *Decoder) ReadBytes(length int) []byte {
	if d.err != nil {
		return nil
	}
	if length < 0 {
		d.err = io.ErrUnexpectedEOF
		return nil
	}

	buf := make([]byte, length)
	_, err := io.ReadFull(d.reader, buf)
	if err != nil {
		d.err = err
		return nil
	}

	return buf
}

func (d *Decoder) ReadString(length int) string {
	return string(d.ReadBytes(length))
}

func (d *Decoder) ReadBytesWithUint32Length() []byte {
	length := d.ReadUint32()
	if d.err != nil {
		return nil
	}
	return d.ReadBytes(int(length))
}

func (d *Decoder) ReadStringWithUint16Length() string {
	length := d.ReadUint16()
	if d.err != nil {
		return ""
	}
	return d.ReadString(int(length))
}
