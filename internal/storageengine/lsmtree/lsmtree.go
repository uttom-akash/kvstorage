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

	var sstables [][]*sstable.SSTable

	for index := 0; index < config.LSMTreeConfig.NumberOfSSTableLevels; index++ {
		sstables = append(sstables, make([]*sstable.SSTable, 0))
	}

	lsmTree := &LSMTree{
		MemTable:   memtable.NewMemTable(),
		SSTables:   sstables,
		sharedChan: channels.GetSharedChannel(),
	}

	return lsmTree
}

func (lsm *LSMTree) Get(key string) *models.Result {

	//check memtable
	result := lsm.MemTable.Get(key)

	if result.Status == models.Found || result.Status == models.Deleted {
		return result
	}

	configs := configs.GetStorageEngineConfig()

	//check sstable
	for level := configs.LSMTreeConfig.FirstLevel; level >= configs.LSMTreeConfig.LastLevel; level-- {

		//read from last since last is the latest
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

func (lsm *LSMTree) Put(key string, value string) {

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
