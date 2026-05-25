package segment

import (
	"errors"
	"fmt"
	"log"
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

	logFile, err := segment.createLog()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateLogFailed, err)
	}
	defer logFile.Close()

	indexFile, err := segment.createIndex()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateIndexFailed, err)
	}
	defer indexFile.Close()

	timeIndexFile, err := segment.createTimeIndex()
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

	nextBytePosition, err := s.getNextBytePosition()
	if err != nil {
		return err
	}

	if s.shouldRollover(nextBytePosition, record) {
		log.Printf("Rolling over segment at offset %d due to size/time limits", offset)
		newSegment, err := s.Rollover(offset)
		if err != nil {
			return err
		}
		s = newSegment
		nextBytePosition = 0
	}

	err = s.appendLog(offset, record)
	if err != nil {
		return err
	}

	err = s.appendIndex(offset, nextBytePosition)
	if err != nil {
		return err
	}

	// TODO: Update to use record's timestamp
	err = s.appendTimeIndex(time.Now().Unix(), offset)
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

	path := s.getLogPath()
	file, err := OpenLogFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	header, err := ReadRecordHeaderAt(file, position)
	if err != nil {
		return nil, err
	}

	if err := ValidateRecordHeader(offset, position, fileSize, header); err != nil {
		return nil, err
	}

	return ReadRecordAt(file, position, header.Length)
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
