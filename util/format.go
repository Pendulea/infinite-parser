package util

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"time"
	"unsafe"
)

func ScheduleTaskEvery(ctx context.Context, d time.Duration, task func()) {
	// Create a ticker to run the task at the specified interval
	ticker := time.NewTicker(d)

	// Start a goroutine to run the task and reschedule it at the specified interval
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Interval task unscheduled")
				return
			case <-ticker.C:
				task()
			}
		}
	}()
}

func ScheduleTask(ctx context.Context, hour, minute int, task func()) {
	// Calculate the duration until the next occurrence of the specified time
	now := time.Now().UTC()
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if now.After(next) {
		// If the next occurrence is already passed today, schedule it for tomorrow
		next = next.Add(24 * time.Hour)
	}
	durationUntilNext := time.Until(next)
	// Create a timer to wait until the next occurrence
	timer := time.NewTimer(durationUntilNext)

	// Start a goroutine to run the task and reschedule it for the next day
	go func() {
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				fmt.Println("Task unscheduled")
				return
			case <-timer.C:
				task()

				// Calculate the duration until the next occurrence and reset the timer
				next = next.Add(24 * time.Hour)
				durationUntilNext = time.Until(next)
				timer.Reset(durationUntilNext)
			}
		}
	}()
}

func SliceSizeInBytes(slice interface{}) int64 {
	// Use reflect to get the underlying value and type of the slice
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		panic("SliceSizeInBytes: provided argument is not a slice")
	}

	// Get the size of the slice header
	sliceHeaderSize := int64(unsafe.Sizeof(reflect.SliceHeader{}))

	// Get the size of one element in the slice
	if v.Len() == 0 {
		return sliceHeaderSize
	}

	elemSize := int64(v.Type().Elem().Size())

	// Calculate the total size: header size + element size * length of slice
	totalSize := sliceHeaderSize + (elemSize * int64(v.Len()))
	return totalSize
}

// int64ToBytes converts an int64 to a byte slice.
func Int64ToBytes(n int64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, n)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

// bytesToInt64 converts a byte slice to an int64.
func BytesToInt64(b []byte) int64 {
	buf := bytes.NewBuffer(b)
	var n int64
	err := binary.Read(buf, binary.BigEndian, &n)
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func RoundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func Len(slice interface{}) (int, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		return v.Len(), nil
	}
	return 0, fmt.Errorf("provided value is not a slice or array")
}

func WriteToFile(filename, content string) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(content)
	return err
}
