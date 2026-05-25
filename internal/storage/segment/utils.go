package segment

import (
	"fmt"
	"os"
	"log"
	"path/filepath"
	"time"

	"github.com/thuhaung/kafka/internal/protocol"
	"github.com/thuhaung/kafka/internal/storage"
)

const (
	OffsetFieldSize = 8
	LengthFieldSize = 4

	HeaderSize = OffsetFieldSize + LengthFieldSize
)

type RecordHeader struct {
	Offset int64
	Length int32
}

func getFileName(baseOffset int64, extension string) string {
	return fmt.Sprintf("%d.%s", baseOffset, extension)
}

func getPartitionPath(segment Segment) string {
	return fmt.Sprintf("%s/%s-%d", segment.LogDir, segment.TopicName, segment.BaseOffset)
}

func (s *Segment) createLog() (*os.File, error) {
	path := s.getLogPath()
	log.Printf("Creating log file for segment at path: %s", path)
	return storage.CreateFile(path)
}

func (s *Segment) createIndex() (*os.File, error) {
	path := s.getIndexPath()
	log.Printf("Creating index file for segment at path: %s", path)
	return storage.CreateFile(path)
}

func (s *Segment) createTimeIndex() (*os.File, error) {
	path := s.getTimeIndexPath()
	log.Printf("Creating time index file for segment at path: %s", path)
	return storage.CreateFile(path)
}

func (s *Segment) getNextBytePosition() (int64, error) {
	path := s.getLogPath()

	info, err := storage.GetFileInfo(path)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (s *Segment) appendLog(offset int64, record []byte) error {
	path := s.getLogPath()
	log.Printf("Appending log entry for offset %d to segment at path: %s", offset, path)

	encoder := protocol.NewEncoder()
	encoder.WriteInt64(offset)
	encoder.WriteInt32(int32(len(record)))
	encoder.WriteBytes(record)

	entry, err := encoder.GetBytes()
	if err != nil {
		return err
	}

	return storage.AppendToFile(path, entry)
}

func (s *Segment) appendIndex(offset int64, position int64) error {
	path := s.getIndexPath()
	log.Printf("Appending index entry for offset %d to segment at path: %s", offset, path)

	encoder := protocol.NewEncoder()
	encoder.WriteInt64(offset)
	encoder.WriteInt64(position)

	entry, err := encoder.GetBytes()
	if err != nil {
		return err
	}

	return storage.AppendToFile(path, entry)
}

func (s *Segment) appendTimeIndex(timestamp int64, offset int64) error {
	path := s.getTimeIndexPath()
	log.Printf("Appending time index entry for timestamp %d and offset %d to segment at path: %s", timestamp, offset, path)

	encoder := protocol.NewEncoder()
	encoder.WriteInt64(timestamp)
	encoder.WriteInt64(offset)

	entry, err := encoder.GetBytes()
	if err != nil {
		return err
	}

	return storage.AppendToFile(path, entry)
}

func (s *Segment) getLogPath() string {
	return filepath.Join(getPartitionPath(*s), getFileName(s.BaseOffset, "log"))
}

func (s *Segment) getIndexPath() string {
	return filepath.Join(getPartitionPath(*s), getFileName(s.BaseOffset, "index"))
}

func (s *Segment) getTimeIndexPath() string {
	return filepath.Join(getPartitionPath(*s), getFileName(s.BaseOffset, "timeindex"))
}

func (s *Segment) lookupOffsetPosition(offset int64) (int64, error) {
	data, err := storage.ReadFile(s.getIndexPath())
	if err != nil {
		return 0, err
	}

	decoder := protocol.NewDecoder(data)
	for decoder.Remaining() > 0 {
		storedOffset := decoder.ReadInt64()
		position := decoder.ReadInt64()

		if err := decoder.GetError(); err != nil {
			return 0, err
		}

		if storedOffset == offset {
			return position, nil
		}
	}

	return 0, ErrOffsetNotFound
}

func (s *Segment) lookupTimestampOffset(timestamp int64) (int64, error) {
	data, err := storage.ReadFile(s.getTimeIndexPath())
	if err != nil {
		return 0, err
	}

	decoder := protocol.NewDecoder(data)
	for decoder.Remaining() > 0 {
		storedTimestamp := decoder.ReadInt64()
		offset := decoder.ReadInt64()

		if err := decoder.GetError(); err != nil {
			return 0, err
		}

		if storedTimestamp == timestamp {
			return offset, nil
		}
	}

	return 0, ErrTimestampNotFound
}

func (s *Segment) shouldRollover(logSize int64, record []byte) bool {
	if s.SegmentBytes > 0 && logSize+HeaderSize+int64(len(record)) > s.SegmentBytes {
		return true
	}

	if s.SegmentMs > 0 && time.Now().Unix()-s.CreatedAt > s.SegmentMs {
		return true
	}

	return false
}

func OpenLogFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func ReadRecordHeaderAt(file *os.File, startPosition int64) (*RecordHeader, error) {
	header := make([]byte, HeaderSize)
	if _, err := file.ReadAt(header, startPosition); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedSegmentLog, err)
	}

	decoder := protocol.NewDecoder(header)
	offset := decoder.ReadInt64()
	recordLength := decoder.ReadInt32()
	if err := decoder.GetError(); err != nil {
		return nil, err
	}

	return &RecordHeader{
		Offset: offset,
		Length: recordLength,
	}, nil
}

func ValidateRecordHeader(expectedOffset int64, position int64, fileSize int64, header *RecordHeader) error {
	if position < 0 || position >= fileSize {
		return ErrInvalidRecordPosition
	}

	if header.Length < 0 {
		return ErrCorruptedSegmentLog
	}

	if header.Offset != expectedOffset {
		return ErrOffsetNotFound
	}

	recordPosition := position + HeaderSize
	recordEndPosition := recordPosition + int64(header.Length)
	if recordEndPosition > fileSize {
		return ErrCorruptedSegmentLog
	}

	return nil
}

func ReadRecordAt(file *os.File, startPosition int64, payloadLength int32) ([]byte, error) {
	record := make([]byte, payloadLength)
	recordPosition := startPosition + HeaderSize
	if _, err := file.ReadAt(record, recordPosition); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedSegmentLog, err)
	}

	return record, nil
}
