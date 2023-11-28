package lsmtree

import (
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/memtable"
	"pkvstore/internal/storageengine/sstable"
	"sync"
)

type LSMTree struct {
	memTable       *memtable.MemTable
	sstTables      [][]*sstable.SSTable
	backgroundChan *SharedChannel
	mutex          sync.RWMutex
}

func NewLSMTree() *LSMTree {

	config := configs.GetStorageEngineConfig()

	var sstables [][]*sstable.SSTable

	for index := 0; index < config.LSMTreeConfig.NumberOfSSTableLevels; index++ {
		sstables = append(sstables, make([]*sstable.SSTable, 0))
	}

	lsmTree := &LSMTree{
		memTable:       memtable.NewMemTable(),
		sstTables:      sstables,
		backgroundChan: NewSharedChannel(),
	}

	go lsmTree.listenForflush()

	return lsmTree
}

func (lsm *LSMTree) Get(key string) *models.Result {

	result := lsm.memTable.Get(key)

	if result.Status == models.Found || result.Status == models.Deleted {
		return result
	}

	configs := configs.GetStorageEngineConfig()

	for level := configs.LSMTreeConfig.FirstLevel; level >= configs.LSMTreeConfig.LastLevel; level-- {

		//read from last since last is the latest
		for sstableId := len(lsm.sstTables[level]) - 1; sstableId >= 0; sstableId-- {
			currentSSTable := lsm.sstTables[level][sstableId]
			if currentSSTable.DoesNotExist(key) {
				continue
			}

			result = currentSSTable.ReadFromSSTable(key)

			if result.Status == models.Found || result.Status == models.Deleted {
				return result
			}
		}
	}

	return models.NewNotFoundResult()
}

func (lsm *LSMTree) Put(key string, value string) {

	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	lsm.memTable.Put(key, value)

	lsm.CheckAndflushAsSSTable()
}

func (lsm *LSMTree) Delete(key string) {

	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	lsm.memTable.Delete(key)

	lsm.CheckAndflushAsSSTable()

}

func (lsm *LSMTree) CheckAndflushAsSSTable() {

	if !lsm.memTable.ShouldFlush() {
		return
	}

	oldMemtable := lsm.memTable

	lsm.memTable = memtable.NewMemTable()

	lsm.backgroundChan.NewMutationEventChannel <- oldMemtable
}
