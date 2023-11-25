package wal

import (
	"encoding/binary"
	"fmt"
	"os"
)

// OperationType represents the type of operation in a WAL entry.
type OperationType byte

const (
	InsertOperation OperationType = iota
	UpdateOperation
	DeleteOperation
)

// LogEntry represents a single entry in the WAL.
type LogEntry struct {
	Operation OperationType
	Key       string
	Value     string
}

// WriteAheadLog represents the Write-Ahead Log on disk.
type WriteAheadLog struct {
	file *os.File
}

// NewWriteAheadLog creates a new Write-Ahead Log instance.
func NewWriteAheadLog(filename string) (*WriteAheadLog, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WriteAheadLog{
		file: file,
	}, nil
}

// WriteEntry appends a LogEntry to the Write-Ahead Log on disk.
func (wal *WriteAheadLog) WriteEntry(entry LogEntry) error {
	if err := binary.Write(wal.file, binary.LittleEndian, entry.Operation); err != nil {
		return err
	}

	keyLength := uint32(len(entry.Key))
	if err := binary.Write(wal.file, binary.LittleEndian, keyLength); err != nil {
		return err
	}

	if _, err := wal.file.Write([]byte(entry.Key)); err != nil {
		return err
	}

	valueLength := uint32(len(entry.Value))
	if err := binary.Write(wal.file, binary.LittleEndian, valueLength); err != nil {
		return err
	}

	if _, err := wal.file.Write([]byte(entry.Value)); err != nil {
		return err
	}

	return nil
}

// Close closes the Write-Ahead Log file.
func (wal *WriteAheadLog) Close() error {
	return wal.file.Close()
}

func main() {
	// Example Usage:

	// Create a Write-Ahead Log
	wal, err := NewWriteAheadLog("write_ahead_log.bin")
	if err != nil {
		fmt.Println("Error creating Write-Ahead Log:", err)
		return
	}
	defer wal.Close()

	// Write entries to the Write-Ahead Log
	entries := []LogEntry{
		{Operation: InsertOperation, Key: "key1", Value: "value1"},
		{Operation: UpdateOperation, Key: "key2", Value: "value2"},
		{Operation: DeleteOperation, Key: "key3", Value: ""},
	}

	for _, entry := range entries {
		if err := wal.WriteEntry(entry); err != nil {
			fmt.Println("Error writing to Write-Ahead Log:", err)
			return
		}
	}
}
