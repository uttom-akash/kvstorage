package memtable

import (
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"sync"
)

type MemTableEntry struct {
	Value       string
	IsTombstone bool
}

func NewMemTableEntry(value string) *MemTableEntry {
	return &MemTableEntry{
		Value:       value,
		IsTombstone: false,
	}
}

type MemTable struct {
	Table map[string]*MemTableEntry
	size  uint32
	mutex sync.RWMutex
}

type FlushableMemTable struct {
	Table map[string]*MemTableEntry
	size  uint32
}

func NewMemTable() *MemTable {
	return &MemTable{
		Table: make(map[string]*MemTableEntry),
		size:  0,
	}
}

func NewFlushableMemTable(table map[string]*MemTableEntry, size uint32) *FlushableMemTable {
	return &FlushableMemTable{
		Table: table,
		size:  size,
	}
}

func (m *MemTable) Put(key string, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Table[key] = NewMemTableEntry(value)
}

func (m *MemTable) Get(key string) *models.Result {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	val, exists := m.Table[key]

	if exists && val.IsTombstone {
		return models.NewDeletedResult()
	}
	if exists {
		return models.NewFoundResult(val.Value)
	}

	return models.NewNotFoundResult()
}

func (m *MemTable) Delete(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Table[key] = NewMemTableEntry("")
	m.Table[key].IsTombstone = true
}

func (m *MemTable) Size() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.Table)
}

func (m *MemTable) ShouldFlush() bool {
	config := configs.GetStorageEngineConfig()

	return m.Size() >= config.MemTableConfig.MaxCapacity
}

func (m *MemTable) ReplaceNewTable() *FlushableMemTable {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	oldTable := NewFlushableMemTable(m.Table, m.size)

	m.Table = make(map[string]*MemTableEntry)
	m.size = 0

	return oldTable
}
