package segment

import (
	"errors"
	"fmt"
	"time"
)

type Segment struct {
	BaseOffset   int64
	CreatedAt    int64
	IsActive     bool
	LatestOffset int64
	SegmentMs    int64
	SegmentBytes int64
	TopicName    string
	LogDir       string
}

const (
	OffsetFieldSize = 8
	LengthFieldSize = 4

	HeaderSize = OffsetFieldSize + LengthFieldSize
)

var (
	ErrCreateLogFailed       = errors.New("Failed to create log file for segment")
	ErrCreateIndexFailed     = errors.New("Failed to create index file for segment")
	ErrCreateTimeIndexFailed = errors.New("Failed to create time index file for segment")
	ErrSegmentInactive       = errors.New("Cannot write to an inactive segment")
	ErrOffsetNotFound        = errors.New("Offset not found in segment")
	ErrTimestampNotFound     = errors.New("Timestamp not found in segment")
	ErrInvalidRecordPosition = errors.New("Invalid record position in segment log")
	ErrCorruptedSegmentLog   = errors.New("Corrupted segment log")
)

func NewSegment(baseOffset int64, topicName string, logDir string, segmentMs int64, segmentBytes int64) (*Segment, error) {
	segment := &Segment{
		BaseOffset:   baseOffset,
		CreatedAt:    time.Now().Unix(),
		IsActive:     true,
		SegmentMs:    segmentMs,
		SegmentBytes: segmentBytes,
		TopicName:    topicName,
		LogDir:       logDir,
	}

	logFile, err := createLog(*segment)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateLogFailed, err)
	}
	defer logFile.Close()

	indexFile, err := createIndex(*segment)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateIndexFailed, err)
	}
	defer indexFile.Close()

	timeIndexFile, err := createTimeIndex(*segment)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateTimeIndexFailed, err)
	}
	defer timeIndexFile.Close()

	return segment, nil
}

func (s *Segment) Append(offset int64, record []byte) error {
	if s.IsActive == false {
		return ErrSegmentInactive
	}

	nextBytePosition, err := getNextBytePosition(*s)
	if err != nil {
		return err
	}

	if s.shouldRollover(nextBytePosition, record) {
		newSegment, err := s.Rollover(offset)
		if err != nil {
			return err
		}
		s = newSegment
		nextBytePosition = 0
	}

	err = appendLog(*s, offset, record)
	if err != nil {
		return err
	}

	err = appendIndex(*s, offset, nextBytePosition)
	if err != nil {
		return err
	}

	// TODO: Update to use record's timestamp
	err = appendTimeIndex(*s, time.Now().Unix(), offset)
	if err != nil {
		return err
	}

	s.LatestOffset = offset

	return nil
}

func (s *Segment) Rollover(offset int64) (*Segment, error) {
	s.IsActive = false
	return NewSegment(offset, s.TopicName, s.LogDir, s.SegmentMs, s.SegmentBytes)
}

func (s *Segment) ReadByOffset(offset int64) ([]byte, error) {
	position, err := s.lookupOffsetPosition(offset)
	if err != nil {
		return nil, err
	}

	file, fileSize, err := s.openLogFile()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	header, err := s.readRecordHeader(file, position)
	if err != nil {
		return nil, err
	}

	if err := s.validateRecordHeader(offset, position, fileSize, header); err != nil {
		return nil, err
	}

	return s.readRecord(file, position, header.Length)
}

func (s *Segment) ReadByTimestamp(timestamp int64) ([]byte, error) {
	offset, err := s.lookupTimestampOffset(timestamp)
	if err != nil {
		return nil, err
	}

	return s.ReadByOffset(offset)
}

// TODO: Implement deletion according to topic retention policy
func Delete() {

}
