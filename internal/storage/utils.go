package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrInvalidFilePath = errors.New("Invalid file path")
)

func ResolveFilePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		var err error
		path, err = filepath.Abs(path)
		if err != nil {
			return path, fmt.Errorf("%w: %v", ErrInvalidFilePath, err)
		}
	}
	return path, nil
}

func ReadFile(path string) ([]byte, error) {
	path, err := ResolveFilePath(path)
	if err != nil {
		return nil, err
	}

	return os.ReadFile(path)
}

func CheckFolderExists(path string) (bool, error) {
	path, err := ResolveFilePath(path)
	if err != nil {
		return false, err
	}
	
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	
	return info.IsDir(), nil
}

func CreateFile(path string) (*os.File, error) {
	path, err := ResolveFilePath(path)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	return os.Create(path)
}

func AppendToFile(path string, data []byte) error {
	path, err := ResolveFilePath(path)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	return err
}

func GetFileInfo(path string) (os.FileInfo, error) {
	path, err := ResolveFilePath(path)
	if err != nil {
		return nil, err
	}
	return os.Stat(path)
}

func GetFilesInDir(path string) ([]os.FileInfo, error) {
	path, err := ResolveFilePath(path)
	if err != nil {
		return nil, err
	}

	exists, err := CheckFolderExists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("Directory does not exist: %s", path)
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var fileInfos []os.FileInfo

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		info, err := file.Info()
		if err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, info)
	}

	return fileInfos, nil
}
