package util

import (
	"errors"
	"fmt"
	"os"
	"time"
)

var ErrAlreadySync = errors.New("already sync")
var ErrFileIsTooRecent = errors.New("file is too recent")
var ErrTimeframeTooSmall = errors.New("timeframe is too small")
var ErrSetNotFound = errors.New("set not found")
var ErrAssetNotFound = errors.New("asset not found")
var ErrAlreadyExists = errors.New("already exists")
var ErrInvalidDataKeyFormat = errors.New("invalid data key format")

type FileLogger struct {
	file *os.File
}

func (f *FileLogger) Close() error {
	return f.file.Close()
}

func NewFileLog(name string) (*FileLogger, error) {
	fp := "./.logs/" + name + time.Now().Format("2006-01-02 15:04:05") + ".log"

	// Open the file in append mode, create it if it doesn't exist
	file, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileLogger{file}, nil
}

func (f *FileLogger) Write(format string, args ...interface{}) error {
	logMessage := fmt.Sprintf(format, args...)
	_, err := f.file.WriteString(logMessage + "\n")
	return err
}
