package lsmtree

import (
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/channels"
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/memtable"
	"pkvstore/internal/storageengine/sstable"
)

type LSMTree struct {
	MemTable   *memtable.MemTable
	SSTables   [][]*sstable.SSTable
	sharedChan *channels.SharedChannel
}

func NewLSMTree() *LSMTree {
	config := configs.GetStorageEngineConfig()

	sstables := make([][]*sstable.SSTable, config.LSMTreeConfig.NumberOfSSTableLevels)

	lsmTree := &LSMTree{
		MemTable:   memtable.NewMemTable(),
		SSTables:   sstables,
		sharedChan: channels.GetSharedChannel(),
	}

	return lsmTree
}

func (lsm *LSMTree) Get(key string) *models.Result {
	result := lsm.MemTable.Get(key)

	if result.Status == models.Found || result.Status == models.Deleted {
		return result
	}

	config := configs.GetStorageEngineConfig()

	for level := config.LSMTreeConfig.FirstLevel; level >= config.LSMTreeConfig.LastLevel; level-- {
		for sstableId := len(lsm.SSTables[level]) - 1; sstableId >= 0; sstableId-- {
			currentSSTable := lsm.SSTables[level][sstableId]

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

func (lsm *LSMTree) Put(key, value string) {
	lsm.MemTable.Put(key, value)
	lsm.notifyNewMutation()
}

func (lsm *LSMTree) Delete(key string) {
	lsm.MemTable.Delete(key)
	lsm.notifyNewMutation()
}

func (lsm *LSMTree) notifyNewMutation() {

	lsm.sharedChan.NewMutationEventChannel <- 1
}
