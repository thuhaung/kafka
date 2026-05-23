package segment

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/thuhaung/kafka/internal/protocol"
	"github.com/thuhaung/kafka/internal/storage"
)

func getFileName(baseOffset int64, extension string) string {
	return fmt.Sprintf("%d.%s", baseOffset, extension)
}

func getPartitionPath(segment Segment) string {
	return fmt.Sprintf("%s/%s-%d", segment.LogDir, segment.TopicName, segment.BaseOffset)
}

func createLog(segment Segment) (*os.File, error) {
	path := getLogPath(segment)
	return storage.CreateFile(path)
}

func createIndex(segment Segment) (*os.File, error) {
	path := getIndexPath(segment)
	return storage.CreateFile(path)
}

func createTimeIndex(segment Segment) (*os.File, error) {
	path := getTimeIndexPath(segment)
	return storage.CreateFile(path)
}

func getNextBytePosition(segment Segment) (int64, error) {
	path := getLogPath(segment)

	info, err := storage.GetFileInfo(path)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func appendLog(segment Segment, offset int64, record []byte) error {
	path := getLogPath(segment)

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

func appendIndex(segment Segment, offset int64, position int64) error {
	path := getIndexPath(segment)

	encoder := protocol.NewEncoder()
	encoder.WriteInt64(offset)
	encoder.WriteInt64(position)

	entry, err := encoder.GetBytes()
	if err != nil {
		return err
	}

	return storage.AppendToFile(path, entry)
}

func appendTimeIndex(segment Segment, timestamp int64, offset int64) error {
	path := getTimeIndexPath(segment)

	encoder := protocol.NewEncoder()
	encoder.WriteInt64(timestamp)
	encoder.WriteInt64(offset)

	entry, err := encoder.GetBytes()
	if err != nil {
		return err
	}

	return storage.AppendToFile(path, entry)
}

func getLogPath(segment Segment) string {
	return filepath.Join(getPartitionPath(segment), getFileName(segment.BaseOffset, "log"))
}

func getIndexPath(segment Segment) string {
	return filepath.Join(getPartitionPath(segment), getFileName(segment.BaseOffset, "index"))
}

func getTimeIndexPath(segment Segment) string {
	return filepath.Join(getPartitionPath(segment), getFileName(segment.BaseOffset, "timeindex"))
}

func (s *Segment) lookupOffsetPosition(offset int64) (int64, error) {
	data, err := storage.ReadFile(getIndexPath(*s))
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
	data, err := storage.ReadFile(getTimeIndexPath(*s))
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

func (s *Segment) openLogFile() (*os.File, int64, error) {
	path, err := storage.ResolveFilePath(getLogPath(*s))
	if err != nil {
		return nil, 0, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, err
	}

	return file, fileInfo.Size(), nil
}

type recordHeader struct {
	Offset int64
	Length int32
}

func (s *Segment) readRecordHeader(file *os.File, position int64) (*recordHeader, error) {
	header := make([]byte, HeaderSize)
	if _, err := file.ReadAt(header, position); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedSegmentLog, err)
	}

	decoder := protocol.NewDecoder(header)
	offset := decoder.ReadInt64()
	recordLength := decoder.ReadInt32()
	if err := decoder.GetError(); err != nil {
		return nil, err
	}

	return &recordHeader{
		Offset: offset,
		Length: recordLength,
	}, nil
}

func (s *Segment) validateRecordHeader(expectedOffset int64, position int64, fileSize int64, header *recordHeader) error {
	if position < 0 || position >= fileSize {
		return ErrInvalidRecordPosition
	}

	if header.Offset != expectedOffset {
		return ErrOffsetNotFound
	}

	if header.Length < 0 {
		return ErrCorruptedSegmentLog
	}

	recordPosition := position + HeaderSize
	recordEndPosition := recordPosition + int64(header.Length)
	if recordEndPosition > fileSize {
		return ErrCorruptedSegmentLog
	}

	return nil
}

func (s *Segment) readRecord(file *os.File, position int64, length int32) ([]byte, error) {
	record := make([]byte, length)
	recordPosition := position + HeaderSize
	if _, err := file.ReadAt(record, recordPosition); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedSegmentLog, err)
	}

	return record, nil
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
