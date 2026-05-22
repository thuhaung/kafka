package segment

import (
	"fmt"
	"os"
	"path/filepath"

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
	fileName := getFileName(segment.BaseOffset, "log")
	path := filepath.Join(getPartitionPath(segment), fileName)

	return storage.CreateFile(path)
}

func createIndex(segment Segment) (*os.File, error) {
	fileName := getFileName(segment.BaseOffset, "index")
	path := filepath.Join(getPartitionPath(segment), fileName)
	return storage.CreateFile(path)
}

func createTimeIndex(segment Segment) (*os.File, error) {
	fileName := getFileName(segment.BaseOffset, "timeindex")
	path := filepath.Join(getPartitionPath(segment), fileName)
	return storage.CreateFile(path)
}

func getNextBytePosition(segment Segment) (int64, error) {
	fileName := getFileName(segment.BaseOffset, "log")
	path := filepath.Join(getPartitionPath(segment), fileName)

	info, err := storage.GetFileInfo(path)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func appendLog(segment Segment, offset int64, record []byte) error {
	fileName := getFileName(segment.BaseOffset, "log")
	path := filepath.Join(getPartitionPath(segment), fileName)
	
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
	fileName := getFileName(segment.BaseOffset, "index")
	path := filepath.Join(getPartitionPath(segment), fileName)

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
	fileName := getFileName(segment.BaseOffset, "timeindex")
	path := filepath.Join(getPartitionPath(segment), fileName)

	encoder := protocol.NewEncoder()
	encoder.WriteInt64(timestamp)
	encoder.WriteInt64(offset)
	
	entry, err := encoder.GetBytes()
	if err != nil {
		return err
	}

	return storage.AppendToFile(path, entry)
}
