package segment

import (
	"errors"
	"fmt"
	"time"
)

type Segment struct {
	BaseOffset int64
	CreatedAt  int64
	IsActive   bool
	LatestOffset int64
	SegmentMs int64
	SegmentBytes int64
	TopicName string
	LogDir string
}

const (
	OffsetFieldSize = 8
	LengthFieldSize = 4
	HeaderSize = OffsetFieldSize + LengthFieldSize
)

var (
	ErrCreateLogFailed = errors.New("Failed to create log file for segment")
	ErrCreateIndexFailed = errors.New("Failed to create index file for segment")
	ErrCreateTimeIndexFailed = errors.New("Failed to create time index file for segment")
	ErrSegmentInactive = errors.New("Cannot write to an inactive segment")
)

func NewSegment(baseOffset int64, topicName string, logDir string, segmentMs int64, segmentBytes int64) (*Segment, error) {
	segment := &Segment{
		BaseOffset: baseOffset,
		CreatedAt: time.Now().Unix(),
		IsActive: true,
		SegmentMs: segmentMs,
		SegmentBytes: segmentBytes,
		TopicName: topicName,
		LogDir: logDir,
	}

	_, err := createLog(*segment)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateLogFailed, err)
	}

	_, err = createIndex(*segment)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateIndexFailed, err)
	}

	_, err = createTimeIndex(*segment)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateTimeIndexFailed, err)
	}

	return segment, nil
}

func (s *Segment) shouldRollover(logSize int64, record []byte) bool {
	if s.SegmentBytes > 0 && logSize + HeaderSize + int64(len(record)) > s.SegmentBytes {
		return true
	}
	
	if s.SegmentMs > 0 && time.Now().Unix() - s.CreatedAt > s.SegmentMs {
		return true
	}

	return false
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

func ReadByOffset() {

}

func ReadByTimestamp() {
}

func Delete() {

}

func Scan() {

}
