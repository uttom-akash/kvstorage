package memtable

import (
	"os"
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/channels"
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
	Table         map[string]*MemTableEntry
	ReadOnlyTable map[string]*MemTableEntry
	size          uint32
	mutex         sync.RWMutex
	sharedChannel *channels.SharedChannel
}

func NewMemTable() *MemTable {
	m := &MemTable{
		Table:         make(map[string]*MemTableEntry),
		ReadOnlyTable: nil,
		size:          0,
		sharedChannel: channels.GetSharedChannel(),
	}

	var wg sync.WaitGroup

	go m.ListenSwitchTableEvent(&wg)

	return m
}

func (m *MemTable) Get(key string) *models.Result {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	val, exists := m.Table[key]

	if !exists {
		val, exists = m.ReadOnlyTable[key]
	}

	if exists && val.IsTombstone {
		return models.NewDeletedResult()
	}
	if exists {
		return models.NewFoundResult(val.Value)
	}

	return models.NewNotFoundResult()
}

func (m *MemTable) Put(key string, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Table[key] = NewMemTableEntry(value)
}

func (m *MemTable) Delete(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Table[key] = NewMemTableEntry("")
	m.Table[key].IsTombstone = true
}

func (m *MemTable) Size() int {
	return len(m.Table)
}

func (m *MemTable) ListenSwitchTableEvent(wg *sync.WaitGroup) {

	defer wg.Done()

	exitSignal := make(chan os.Signal, 1)

	for {
		select {
		case switchevent := <-m.sharedChannel.SwitchMemtableEvent:
			if switchevent < 0 {
				continue
			}

			m.swtichMemtable()

			m.sharedChannel.FlushMemtableEvent <- 1
		case <-exitSignal:
			return
		}
	}
}

func (m *MemTable) swtichMemtable() {

	config := configs.GetStorageEngineConfig()

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Size() < config.MemTableConfig.MaxCapacity || m.ReadOnlyTable != nil {
		return
	}

	m.ReadOnlyTable = m.Table
	m.Table = make(map[string]*MemTableEntry)
}

func (m *MemTable) ClearReadOnlyMemtable() {
	m.ReadOnlyTable = nil
}
